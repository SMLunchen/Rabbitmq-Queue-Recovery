#!/usr/bin/env python3
"""
RabbitMQ Queue File Recovery Tool
Extracts messages using binary markers from .qs files
"""

import os
import sys
import argparse
import pika
from datetime import datetime

class BinaryMarkerQueueRecovery:
    def __init__(self, config):
        self.config = config
        self.qs_files = []

        # Connect to RabbitMQ if not in dry-run mode
        if not config.dry_run:
            self.connect_to_rabbitmq()

    def connect_to_rabbitmq(self):
        """Establish connection to RabbitMQ server"""
        credentials = pika.PlainCredentials(self.config.username, self.config.password)
        parameters = pika.ConnectionParameters(
            host=self.config.host,
            port=self.config.port,
            virtual_host=self.config.vhost,
            credentials=credentials
        )

        try:
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Declare the queue if it doesn't exist
            self.channel.queue_declare(queue=self.config.queue, durable=True)

            print(f"Connected to RabbitMQ on {self.config.host}:{self.config.port}, vhost: {self.config.vhost}")
        except Exception as e:
            print(f"Failed to connect to RabbitMQ: {str(e)}")
            sys.exit(1)

    def scan_files(self, directory):
        """Scan directory for queue segment files"""
        self.qs_files = []
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if filename.endswith('.qs'):
                self.qs_files.append(filepath)

        # Sort files by sequence number
        self.qs_files.sort(key=lambda f: int(os.path.basename(f).split('.')[0]))
        print(f"Found {len(self.qs_files)} segment files")

    def print_hex_dump(self, data, start=0, length=64):
        """Print a hex dump of binary data"""
        for i in range(0, min(length, len(data)), 16):
            chunk = data[i:i+16]
            hex_part = ' '.join(f'{b:02x}' for b in chunk)
            ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in chunk)
            print(f"{start+i:08x}  {hex_part.ljust(48)} | {ascii_part}")

    def clean_message_content(self, raw_content):
        """
        Clean message content by removing Erlang term artifacts.
        Handles JSON, XML, and plain text.
        """
        # Strategy 1: If it looks like JSON
        if b'{' in raw_content:
            json_start = raw_content.find(b'{')
            # Find the matching closing brace
            json_end = self.find_json_end(raw_content, json_start)
            if json_end > json_start:
                return raw_content[json_start:json_end]
        
        # Strategy 2: If it looks like XML
        if b'<' in raw_content:
            xml_start = raw_content.find(b'<')
            xml_end = raw_content.rfind(b'>')
            if xml_end > xml_start:
                return raw_content[xml_start:xml_end+1]
        
        # Strategy 3: Look for printable ASCII/UTF-8 content
        # Remove leading/trailing non-printable bytes
        content = raw_content.strip()
        
        # Remove trailing Erlang artifacts (common patterns)
        # Pattern: "jt" followed by bytes (Erlang NIL_EXT + SMALL_TUPLE_EXT)
        if b'jt' in content[-20:]:
            jt_pos = content.rfind(b'jt')
            content = content[:jt_pos]
        
        # Remove trailing base64-like garbage after valid content
        # This happens when Erlang binary IDs are appended
        # Look for pattern: valid content + noise
        try:
            # Try to decode as UTF-8
            decoded = content.decode('utf-8', errors='ignore')
            # Find where printable content ends
            printable_end = len(decoded)
            for i in range(len(decoded)-1, -1, -1):
                if decoded[i].isprintable() or decoded[i] in '\n\r\t':
                    printable_end = i + 1
                    break
            content = decoded[:printable_end].encode('utf-8')
        except:
            pass
        
        return content
    
    def find_json_end(self, data, start):
        """
        Find the end of a JSON structure by counting braces.
        Returns position after the closing brace.
        """
        depth = 0
        in_string = False
        escape = False
        
        for i in range(start, len(data)):
            byte = data[i]
            char = chr(byte) if byte < 128 else None
            
            if escape:
                escape = False
                continue
            
            if char == '\\' and in_string:
                escape = True
                continue
            
            if char == '"':
                in_string = not in_string
                continue
            
            if not in_string:
                if char == '{':
                    depth += 1
                elif char == '}':
                    depth -= 1
                    if depth == 0:
                        return i + 1
        
        return len(data)

    def extract_messages_from_erlang_entries(self, file_path):
        """
        Extract messages by parsing Erlang term structure.
        Uses knowledge of .qs file format.
        """
        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()

            print(f"Processing {os.path.basename(file_path)}: {len(file_data)} bytes")

            # Skip RCQS header (first 64 bytes)
            pos = 64
            messages_published = 0

            while pos < len(file_data):
                # Read entry header (8 bytes)
                if pos + 8 > len(file_data):
                    break
                
                # Parse entry header: Size(4) + Flags(1) + CRC16(2) + Reserved(1)
                size = int.from_bytes(file_data[pos:pos+4], byteorder='big')
                flags = file_data[pos+4]
                
                # Sanity check
                if size == 0 or size > 10*1024*1024:  # Max 10MB per message
                    pos += 8
                    continue
                
                pos += 8  # Move past header
                
                if pos + size > len(file_data):
                    break
                
                # Extract message data
                msg_data = file_data[pos:pos+size]
                pos += size
                
                # Parse Erlang term (starts with 0x83 for external term format)
                if len(msg_data) > 0 and msg_data[0] == 0x83:
                    # Skip Erlang term format marker
                    msg_data = msg_data[1:]
                
                # Look for the actual payload inside the Erlang structure
                # Common pattern: after various Erlang type tags, there's a binary (0x6d = 'm')
                # followed by 4-byte length, then the actual content
                
                payload = self.extract_payload_from_erlang(msg_data)
                
                if payload and len(payload) > 10:
                    # Clean the payload
                    clean_payload = self.clean_message_content(payload)
                    
                    try:
                        content_str = clean_payload.decode('utf-8', errors='strict')
                        
                        # Final validation: should look like real content
                        if self.is_valid_content(content_str):
                            if self.config.verbose and messages_published < 3:
                                print(f"\n  Message {messages_published+1}:")
                                print(f"    Length: {len(content_str)} bytes")
                                print(f"    Preview: {content_str[:150]}...")
                            
                            # Publish the content
                            if not self.config.dry_run:
                                self.channel.basic_publish(
                                    exchange=self.config.exchange,
                                    routing_key=self.config.routing_key or self.config.queue,
                                    body=content_str,
                                    properties=pika.BasicProperties(
                                        delivery_mode=2,
                                        content_type='application/json' if content_str.strip().startswith('{') else 'text/plain'
                                    )
                                )
                            
                            messages_published += 1
                            
                            # Report progress
                            if messages_published % 500 == 0:
                                print(f"  Progress: {messages_published} messages published")
                            
                            # Check message limit
                            if self.config.message_limit > 0 and messages_published >= self.config.message_limit:
                                break
                    
                    except UnicodeDecodeError:
                        if self.config.verbose:
                            print(f"  Skipping message - invalid UTF-8")

            print(f"  Published {messages_published} messages from {os.path.basename(file_path)}")
            return messages_published

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            if self.config.verbose:
                import traceback
                traceback.print_exc()
            return 0

    def extract_payload_from_erlang(self, data):
        """
        Extract payload from Erlang term structure.
        Looks for BINARY_EXT (0x6d = 'm') tags which contain the actual message.
        """
        pos = 0
        largest_binary = b''
        
        while pos < len(data) - 5:
            tag = data[pos]
            
            # BINARY_EXT (0x6d = 'm')
            if tag == 0x6d or tag == ord('m'):
                # Next 4 bytes are the length
                if pos + 5 <= len(data):
                    length = int.from_bytes(data[pos+1:pos+5], byteorder='big')
                    
                    # Sanity check
                    if 10 <= length <= 1024*1024 and pos + 5 + length <= len(data):
                        binary_data = data[pos+5:pos+5+length]
                        
                        # Keep the largest binary chunk that looks like content
                        if len(binary_data) > len(largest_binary):
                            # Basic heuristic: should contain some printable chars
                            printable_count = sum(1 for b in binary_data[:100] if 32 <= b <= 126)
                            if printable_count > 10:
                                largest_binary = binary_data
                        
                        pos += 5 + length
                        continue
            
            pos += 1
        
        return largest_binary

    def is_valid_content(self, content):
        """Check if content looks like valid message data"""
        if len(content) < 10:
            return False
        
        # Should have reasonable amount of printable characters
        printable = sum(1 for c in content[:200] if c.isprintable() or c in '\n\r\t')
        if printable < len(content[:200]) * 0.7:
            return False
        
        # Should not be mostly base64 artifacts
        if content.strip().replace('\n', '').replace('\r', '').isalnum() and len(content) > 100:
            # Might be base64 garbage
            return False
        
        return True

    def process_files(self):
        """Process all queue segment files"""
        total_published = 0

        # Process files one by one
        for qs_file in self.qs_files:
            if self.config.file_limit > 0 and len(self.qs_files[:self.qs_files.index(qs_file)+1]) > self.config.file_limit:
                print(f"Reached file limit ({self.config.file_limit}), stopping.")
                break

            count = self.extract_messages_from_erlang_entries(qs_file)
            total_published += count

            # Check message limit
            if self.config.message_limit > 0 and total_published >= self.config.message_limit:
                print(f"Reached message limit ({self.config.message_limit}), stopping.")
                break

        print(f"\n{'='*60}")
        print(f"Total messages published: {total_published}")
        print(f"{'='*60}")

    def cleanup(self):
        """Close connections and perform cleanup"""
        if not self.config.dry_run and hasattr(self, 'connection'):
            self.connection.close()
            print("RabbitMQ connection closed")

def parse_args():
    parser = argparse.ArgumentParser(description='Extract and publish RabbitMQ messages from queue backup files')
    parser.add_argument('--dir', default='.', help='Directory containing queue files')
    parser.add_argument('--host', default='localhost', help='RabbitMQ host')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ port')
    parser.add_argument('--vhost', default='/', help='RabbitMQ virtual host')
    parser.add_argument('--username', default='guest', help='RabbitMQ username')
    parser.add_argument('--password', default='guest', help='RabbitMQ password')
    parser.add_argument('--queue', required=True, help='Target queue name')
    parser.add_argument('--exchange', default='', help='Target exchange name (default: default exchange)')
    parser.add_argument('--routing-key', default='', help='Routing key for publishing (default: queue name)')
    parser.add_argument('--dry-run', action='store_true', help='Process files but do not publish messages')
    parser.add_argument('--file-limit', type=int, default=0, help='Maximum number of files to process')
    parser.add_argument('--message-limit', type=int, default=0, help='Maximum number of messages to publish')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')

    return parser.parse_args()

def main():
    args = parse_args()

    print(f"RabbitMQ Queue Recovery - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Processing files in directory: {args.dir}")

    # Create and run the recovery process
    recovery = BinaryMarkerQueueRecovery(args)
    recovery.scan_files(args.dir)
    recovery.process_files()
    recovery.cleanup()

    print("Recovery process completed")

if __name__ == "__main__":
    main()
