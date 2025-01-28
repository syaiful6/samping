## Queue

The queue

The scope contains

- type (Unicode string) - `queue`
- name: (string): The source of this queue message originated from
- headers - (Iterable): An iterable of \[name, value\] two-item iterables, where name is the header name and value is the header value.
- properties - (Iterable): An iterable of \[name, value\] two-item iterables, where name is the header name and value is the header value.
- broker - (string): The broker that send this queue to application
- body - (string|byte): The body of the message

Ack - `send` event

Send by the application to send information to broker that the application acknowledged this queue message.

- type (Unicode string) - `queue.ack`

Nack - `send` event

Send by the application to send information to the broker that the application not acknowledged this queue message.

- type (Unicode string) - `queue.ack`
- delay (int) - Delay in seconds before this queue message

## Cron

The scope contains:

- type (Unicode string) - `beat`
- time (string) - datetime string in iso8601 format.