# RabbitMQ Queue Recovery Tool

## Overview

This tool enables the recovery of RabbitMQ messages directly from binary queue storage files. It's particularly useful for data restoration from backups (such as VMware/Veeam) or when dealing with corrupted RabbitMQ installations.
Mainly this tool addresses the issue, when a queue thats not quorum gets emptied through a restart or crash. Rabbitmq will then flush all the data in the queue and you will see messages like:

`2025-09-23 06:03:52.580396+00:00 [warning] <0.690.0> Queue name1 in vhost xzy dropped 0/0/0 persistent messages and 0 transient messages after unclean shutdown`

`2025-09-23 06:03:53.157576+00:00 [warning] <0.699.0> Queue name2 in vhost xzy dropped 0/0/0 persistent messages and 435670 transient messages after unclean shutdown`

`2025-09-23 06:03:53.336337+00:00 [warning] <0.702.0> Queue name3 in vhost xzy dropped 0/0/0 persistent messages and 626079 transient messages after unclean shutdown`

`2025-09-23 06:03:53.485631+00:00 [warning] <0.696.0> Queue name4 in vhost xzy dropped 0/0/0 persistent messages and 853336 transient messages after unclean shutdown`

`2025-09-23 06:03:53.926845+00:00 [warning] <0.693.0> Queue name5 in vhost xzy dropped 0/0/0 persistent messages and 1915087 transient messages after unclean shutdown`


The tool identifies messages by detecting binary markers that indicate the beginning and end of message blocks, and then sends these messages to a functioning RabbitMQ server.
You cannot copy the binary files just back, as rabbitmq will not pick them up. - Also there is no documented restore to get queue data back after such a situation. - For the future make sure, your queues are set to quorum.
You will need a full backup of your system (e.g. veeam restore) - you MUST NOT power up that backup, since the rabbitmq service will come up and flush your queues again...!

Follow this manual to bring your data back... 

## Requirements

- Python 3.6 or higher
- pika (Python library for RabbitMQ)

Install dependencies:

```bash
pip install pika
```

## Use Cases

- Recovering messages from backups (VMware, Veeam, etc.)
- Data recovery from corrupted RabbitMQ installations

## Important: Safety Notice

**NEVER** start RabbitMQ while you have source files from a backup in the RabbitMQ data directories. This could lead to data loss or corruption. Always restore files to a separate directory and use this tool to extract the messages.

## Usage

### Basic Syntax

```bash
python rabbitmq_recovery.py --dir /path/to/queue/files --queue target_queue_name [options]
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `--dir` | Directory containing queue files (*.qs) |
| `--queue` | Name of the target queue on the RabbitMQ server |
| `--host` | RabbitMQ host (default: localhost) |
| `--port` | RabbitMQ port (default: 5672) |
| `--vhost` | RabbitMQ virtual host (default: /) |
| `--username` | RabbitMQ username (default: guest) |
| `--password` | RabbitMQ password (default: guest) |
| `--exchange` | Exchange name (default: default exchange) |
| `--routing-key` | Routing key (default: queue name) |
| `--dry-run` | Test run without actually publishing |
| `--file-limit` | Maximum number of files to process |
| `--message-limit` | Maximum number of messages to publish |
| `-v, --verbose` | Show detailed output |

### Examples

**Test run with verbose output:**
```bash
python rabbitmq_recovery.py --dir /mnt/backup/rabbitmq/queues --queue my_recovered_queue --dry-run --verbose
```

**Restore messages to local RabbitMQ server:**
```bash
python rabbitmq_recovery.py --dir /mnt/backup/rabbitmq/queues --queue my_recovered_queue
```

**Restore messages to a remote RabbitMQ server with authentication:**
```bash
python rabbitmq_recovery.py --dir /mnt/backup/rabbitmq/queues --queue my_recovered_queue --host rabbitmq-server.example.com --vhost production --username admin --password secure_password

```

**Limit processing:**
```bash
python rabbitmq_recovery.py --dir /mnt/backup/rabbitmq/queues --queue my_recovered_queue --file-limit 10 --message-limit 5000
```

## Recovering from a Backup

To recover messages from a backup (VMware, Veeam, etc.):

1. Restore the RabbitMQ data files from your backup to a temporary directory. With VMware or Veeam, you can extract specific files from a backup. Or you use a disk image and mount it. Knoppix can also work. Make the data accessible. 

2. Locate the source queue files in the restored directory tree. The typical path is:
   ```
   /var/lib/rabbitmq/mnesia/rabbit@[hostname]/msg_stores/vhosts/[vhost-hash]/queues/[queue-hash]/
   ```

3. In this directory, you'll find files with `.qs` and `.qi` extensions. The `.qs` files contain the actual message data.

4. Use the tool to extract the messages and publish them to your RabbitMQ server. - I would suggest you restore to a completely different queue and then use shovel to bring the data back to your queue that lost the data. So you can analyze the data that you restored.

## How It Works

The tool analyzes the binary `.qs` files and looks for special patterns that mark the beginning (`6d 00 00 0`) and end (`74 00 00 00`) of message blocks. The message data between these markers is extracted and sent to the target RabbitMQ server.

This method bypasses the need to start RabbitMQ and allows direct extraction of messages from binary files - perfect for backup restoration or data recovery situations.

## Troubleshooting

If the tool doesn't find any messages:

1. Check that you're using the correct files. Only `.qs` files contain the actual message data.

2. Make sure the files aren't corrupted. If files are corrupted, partial recovery might still be possible.

3. Try enabling `--verbose` mode to see more details about the processing.

4. Check if the messages follow a different format than expected. In this case, adjusting the source code might be necessary.
