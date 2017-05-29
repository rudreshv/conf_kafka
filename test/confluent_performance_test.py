# """
# @author : Rudresh
# @Created on: 17/04/17
# """
#
# import time,confluent_kafka
#
# msg_count = 10
# msg_payload = "8.8499054139975e+21,0.669444469508,0.359820642344,0.439582690849,0.884766793493,0.606697656317,0.532459263809,0.0072599257356,0.553420640697,0.432341538346,0.95494511953,0.591462415387,0.883209519161,0.60251350089,0.8323370339,0.321162777704,0.746463728566,0.516891258484,0.947631767516,0.164882181501,0.332779445904,0.161235006441,0.296866573594,0.789258918883,0.620385674389,0.300208451287,0.0900348895356,0.602643791353,0.275942129835,0.818078217372,0.578670248407,0.479801787954,0.476777305542,0.118408275773,0.259805304214,0.783203398874,0.000431679556757,0.286971553882,0.495127831661,0.293020970959,0.905915856449,0.239751045491,0.548554586071,0.39923415486,0.248005265183,0.544510725219,0.30117656583,0.0306735219838,0.204608230505,0.898038942372,0.17444423715,0.0671972679349,0.607820449357,0.200922655958,0.380746147853,0.891630893865,0.829526005322,0.0290808104043,0.844119504391,0.174793618318,0.580233793867,0.941557531549,0.240868611674,0.723839951329,0.158766825449,0.129142336158,0.0656538407799,0.836486836452,0.349827776231,0.278522898652,0.242006400062,0.397504245798,0.867884710715,0.537567187572,0.0266923524397,0.626547485986,0.154323853798,0.0663900532219,0.0800311957073,0.457036015012,0.36419597116,0.448142170785,0.448042140655,0.467926467072,0.283584988985,0.100877457253,0.29879982137,0.827495472738,0.229439233441,0.104390023155,0.701063041904,0.537166991015,0.0603082625189,0.315959901329,0.088671116988,0.227857754212,0.946592549761,0.725507426552,0.589680545677,0.160116399214,0.682808151515"
# bootstrap_servers = 'localhost:9092'
# topic = 'abcd14'
#
#
# def confluent_kafka_producer_performance():
#     conf = {'bootstrap.servers': bootstrap_servers,
#             'queue.buffering.max.messages': msg_count,
#             # 'queue.buffering.max.ms': 900000
#             }
#     producer = confluent_kafka.Producer(**conf)
#     messages_to_retry = 0
#
#     producer_start = time.time()
#     for i in range(msg_count):
#         try:
#             producer.produce(topic, value=msg_payload)
#             producer.poll(0)
#         except BufferError as e:
#             messages_to_retry += 1
#
#     # hacky retry messages that over filled the local buffer
#     print(messages_to_retry)
#     for i in range(messages_to_retry):
#         try:
#             producer.produce(topic, value=msg_payload)
#             producer.poll(0)
#         except BufferError as e:
#             producer.produce(topic, value=msg_payload)
#             producer.poll(0)
#
#     producer.flush()
#
#     return time.time() - producer_start
#
# producer_timings = {}
# producer_timings['confluent_kafka_producer'] = confluent_kafka_producer_performance()
# print(producer_timings)
#
# import confluent_kafka
#
#
# def confluent_kafka_consumer_performance():
#     msg_consumed_count = 0
#     conf = {'bootstrap.servers': bootstrap_servers,
#             'group.id': 2,
#             'session.timeout.ms': 0.5*msg_count,
#             'default.topic.config': {
#                 'auto.offset.reset': 'earliest'
#             }
#             }
#
#     consumer = confluent_kafka.Consumer(**conf)
#
#     consumer_start = time.time()
#     # This is the same as pykafka, subscribing to a topic will start a background thread
#     consumer.subscribe([topic])
#
#     while True:
#         msg = consumer.poll(1)
#         if msg:
#             msg_consumed_count += 1
#         # print(msg_consumed_count)
#         if msg_consumed_count >= msg_count:
#             break
#
#     consumer_timing = time.time() - consumer_start
#     consumer.close()
#     return consumer_timing
#
#
# # consumer_timings = {}
# # consumer_timings['confluent_kafka_consumer'] = confluent_kafka_consumer_performance()
# # print(consumer_timings)
