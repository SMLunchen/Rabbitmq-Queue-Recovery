#!/usr/bin/env python3
import os
import sys
import re
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

    def extract_and_publish_exact_markers(self, file_path):
        """Extract messages using exact binary markers"""
        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()

            print(f"Processing {os.path.basename(file_path)}: {len(file_data)} bytes")

            # Skip RCQS header (first 64 bytes)
            data = file_data[64:]

            # Define exact markers based on your input
            # Start marker: 6d 00 00 0
            # End marker: 74 00 00 00 00

            # Find all occurrences of the start marker
            start_positions = []
            for i in range(len(data) - 4):
                # Check for binary pattern match
                if data[i] == 0x6d and data[i+1] == 0x00 and data[i+2] == 0x00 and data[i+3] == 0x00:
                    start_positions.append(i)
                # Also check for ASCII 'm' (0x6d) variant
                elif data[i] == ord('m') and data[i+1] == 0x00 and data[i+2] == 0x00 and data[i+3] == 0x00:
                    start_positions.append(i)

            # Find all occurrences of the end marker
            end_positions = []
            for i in range(len(data) - 4):
                # Check for binary pattern match
                if data[i] == 0x74 and data[i+1] == 0x00 and data[i+2] == 0x00 and data[i+3] == 0x00:
                    end_positions.append(i)
                # Also check for ASCII 't' (0x74) variant
                elif data[i] == ord('t') and data[i+1] == 0x00 and data[i+2] == 0x00 and data[i+3] == 0x00:
                    end_positions.append(i)

            print(f"  Found {len(start_positions)} start markers and {len(end_positions)} end markers")

            messages_published = 0

            # Match start and end markers to extract message blocks
            for i, start_pos in enumerate(start_positions):
                try:
                    # Find the next end marker after this start position
                    next_end_pos = None
                    for end_pos in end_positions:
                        if end_pos > start_pos:
                            next_end_pos = end_pos
                            break

                    if next_end_pos is not None:
                        # Extract the message content
                        # Look for JSON start after header marker
                        json_start = data.find(b'{', start_pos)

                        if json_start > start_pos and json_start < next_end_pos:
                            # Look for JSON end before footer marker
                            # We're looking for "}}" which typically precedes the 't' footer
                            json_end = data.rfind(b'}}', json_start, next_end_pos)

                            if json_end > json_start:
                                json_end += 2  # Include the closing braces
                                message_content = data[json_start:json_end]

                                # Show hex dump for debugging
                                if self.config.verbose and messages_published < 3:
                                    print(f"\n  Message {messages_published+1} hex dump:")
                                    self.print_hex_dump(message_content, json_start, min(128, len(message_content)))

                                # Convert to string
                                try:
                                    content_str = message_content.decode('utf-8', errors='replace')

                                    # Publish the content
                                    if not self.config.dry_run:
                                        self.channel.basic_publish(
                                            exchange=self.config.exchange,
                                            routing_key=self.config.routing_key or self.config.queue,
                                            body=content_str,
                                            properties=pika.BasicProperties(
                                                delivery_mode=2,
                                                content_type='application/json'
                                            )
                                        )

                                    messages_published += 1

                                    # Show sample in verbose mode
                                    if self.config.verbose and messages_published <= 3:
                                        print(f"  Message {messages_published} content: {content_str[:100]}...")

                                except UnicodeDecodeError:
                                    # Skip if we can't decode
                                    if self.config.verbose and messages_published < 3:
                                        print(f"  Skipping message {messages_published+1} - cannot decode as UTF-8")

                        # Report progress
                        if messages_published % 500 == 0 and messages_published > 0:
                            print(f"  Progress: {messages_published} messages published")

                except Exception as e:
                    print(f"  Error processing message at position {start_pos}: {e}")
                    continue

            print(f"  Published {messages_published} messages from {os.path.basename(file_path)}")
            return messages_published

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return 0

    def process_files(self):
        """Process all queue segment files"""
        total_published = 0

        # Process files one by one
        for qs_file in self.qs_files:
            if self.config.file_limit > 0 and len(self.qs_files[:self.qs_files.index(qs_file)+1]) > self.config.file_limit:
                print(f"Reached file limit ({self.config.file_limit}), stopping.")
                break

            count = self.extract_and_publish_exact_markers(qs_file)
            total_published += count

            # Check message limit
            if self.config.message_limit > 0 and total_published >= self.config.message_limit:
                print(f"Reached message limit ({self.config.message_limit}), stopping.")
                break

        print(f"Total messages published: {total_published}")

    def cleanup(self):
        """Close connections and perform cleanup"""
        if not self.config.dry_run and hasattr(self, 'connection'):
            self.connection.close()
            print("RabbitMQ connection closed")

def parse_args():
    parser = argparse.ArgumentParser(description='Extract and publish RabbitMQ messages using binary markers')
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

    print(f"RabbitMQ Queue Recovery with Exact Binary Markers - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Processing files in directory: {args.dir}")

    # Create and run the recovery process
    recovery = BinaryMarkerQueueRecovery(args)
    recovery.scan_files(args.dir)
    recovery.process_files()
    recovery.cleanup()

    print("Recovery process completed")

if __name__ == "__main__":
    main()
