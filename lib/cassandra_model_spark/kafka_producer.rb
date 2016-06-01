class Kafka::Producer
  def send_msgs(msgs)
    msgs = msgs.map do |(topic, key, msg)|
      KeyedMessage.new(topic, key, msg)
    end
    producer.send(java.util.ArrayList.new(msgs))
  end
end
