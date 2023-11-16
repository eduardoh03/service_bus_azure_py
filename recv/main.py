import os
from dotenv import load_dotenv
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.identity.aio import DefaultAzureCredential

# Carregar variáveis de ambiente a partir do arquivo .env
load_dotenv()

FULLY_QUALIFIED_NAMESPACE = os.getenv("FULLY_QUALIFIED_NAMESPACE")
QUEUE_NAME = os.getenv("QUEUE_NAME")

credential = DefaultAzureCredential()


async def run():
    # create a Service Bus client using the connection string
    async with ServiceBusClient.from_connection_string(
            conn_str=FULLY_QUALIFIED_NAMESPACE,
            logging_enable=True) as service_bus_client:
        async with service_bus_client:
            # get the Queue Receiver object for the queue
            receiver = service_bus_client.get_queue_receiver(queue_name=QUEUE_NAME)
            async with receiver:
                received_msgs = await receiver.receive_messages(max_wait_time=5, max_message_count=20)
                for msg in received_msgs:
                    print("Received: " + str(msg))
                    # complete the message so that the message is removed from the queue
                    await receiver.complete_message(msg)


if __name__ == '__main__':
    asyncio.run(run())
