from datetime import timedelta

from samping.driver.sqs import Qos
from samping.messages import Message
from samping.utils.time import utcnow


def test_qos_get_unacked_messages():
    qos = Qos(50, visibility_timeout=300)

    for i in range(10):
        message = Message(
            "",
            properties={
                "delivery_info": {
                    "message_id": i,
                    "delivered_at": utcnow() - timedelta(seconds=320 + i),
                }
            },
        )
        qos.append(message)

    for i in range(11, 20):
        message = Message(
            "",
            properties={
                "delivery_info": {
                    "message_id": i,
                    "delivered_at": utcnow() - timedelta(seconds=i),
                }
            },
        )
        qos.append(message)

    assert qos.can_consume()

    # this messages need to be removed
    messages = qos.get_unacked_messages()
    assert len(messages) == len(list(range(10)))

    qos.remove_unacked_messages()

    assert len(qos) == len(list(range(11, 20)))
