import kafkaProducer

if __name__ == '__main__':
    # parse command line arguments
    args = parse_command_line_arguments()

    # init producer
    producer = create_kafka_producer(args.bootstrap_server)

    # init dictionary of namespaces
    namespace_dict = init_namespaces()

    # used to parse user type
    user_types = {True: 'bot', False: 'human'}

    # consume websocket
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'

    print('Messages are being published to Kafka topic')
    messages_count = 0

    for event in EventSource(url):
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
            except ValueError:
                pass
            else:
                # filter out events, keep only article edits (mediawiki.recentchange stream)
                if event_data['type'] == 'edit':
                    # construct valid json event
                    event_to_send = construct_event(event_data, user_types)

                    producer.send('wikipedia-events', value=event_to_send)

                    messages_count += 1

        if messages_count >= args.events_to_produce:
            print('Producer will be killed as {} events were producted'.format(args.events_to_produce))
            exit(0)

    # See PyCharm help at https://www.jetbrains.com/help/pycharm/
