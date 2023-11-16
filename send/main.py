import os
from dotenv import load_dotenv
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
from azure.identity.aio import DefaultAzureCredential

# Carregar variáveis de ambiente a partir do arquivo .env
load_dotenv()
FULLY_QUALIFIED_NAMESPACE = os.getenv("FULLY_QUALIFIED_NAMESPACE")
QUEUE_NAME = os.getenv("QUEUE_NAME")

credential = DefaultAzureCredential()


async def send_single_message(sender):
    # Create a Service Bus message and send it to the queue
    message = ServiceBusMessage("{"'id'": 1, "'"name"'": "'"Fernando"'"}}")
    await sender.send_messages(message)
    print("Sent a single message")


async def send_a_list_of_messages(sender):
    # Create a list of messages and send it to the queue
    messages = [ServiceBusMessage("Mandando cinco mensagens") for _ in range(5)]
    await sender.send_messages(messages)
    print("Sent a list of 5 messages")


async def send_batch_message(sender):
    # Create a batch of messages
    async with sender:
        batch_message = await sender.create_message_batch()
        for _ in range(10):
            try:
                # Add a message to the batch
                batch_message.add_message(ServiceBusMessage("Mandando 10 mensagens"))
            except ValueError:
                # ServiceBusMessageBatch object reaches max_size.
                # New ServiceBusMessageBatch object can be created here to send more data.
                break
        # Send the batch of messages to the queue
        await sender.send_messages(batch_message)
    print("Sent a batch of 10 messages")


async def run():
    # create a Service Bus client using the connection string
    async with ServiceBusClient.from_connection_string(
            conn_str=FULLY_QUALIFIED_NAMESPACE,
            logging_enable=True) as service_bus_client:
        # Get a Queue Sender object to send messages to the queue
        sender = service_bus_client.get_queue_sender(queue_name=QUEUE_NAME)
        async with sender:
            # Send one message
            await send_single_message(sender)
            # Send a list of messages
            await send_a_list_of_messages(sender)
            # Send a batch of messages
            await send_batch_message(sender)


if __name__ == '__main__':
    asyncio.run(run())
