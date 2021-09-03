import configparser
import json
import datetime
import time
import logging
import pprint
import pyodbc 
from kafka import KafkaConsumer
import sql_server_connector

INT = 'int'
STRING = 'string'

def stream_rsvp(topic_name, server, cursor):
    '''
    stream meetup data with kafka into rsvp data model in sql server - rsvp & group_topics tables
    '''
    try:
        for msg in KafkaConsumer(topic_name,bootstrap_servers=[server]):
            events = msg.value.decode('utf8')
            json_events = json.loads(events)
            insert_rsvp(cursor, json_events)
            insert_group_topics(cursor, json_events.get('group').get('group_topics'), json_events.get('rsvp_id'))
    except Exception as ex:
        logging.error(f'Error occured {ex}')

def get_nested_value(obj, nesting, data_type):
    '''
    get value from nested dictionary while handeling missing values and type casting
    obj: the dictionary
    nesting: order of extraction
    data_type: data type
    '''
    next = obj.get(nesting[0])
    if isinstance(next, dict):
        return get_nested_value(next, nesting[1:], data_type)
    elif next is None:
        return None
    else: # add more types here
        if data_type == STRING:
            value = str(next) 
        elif data_type == INT:
            value = int(next)
        else:
            value = str(next)
        return value


def insert_rsvp(cursor, rsvp):
    '''
    insert data to dbo.rsvp table 
    '''
    cursor.execute("INSERT INTO dbo.rsvp VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                get_nested_value(rsvp, ['event', 'event_id'], STRING),
                                get_nested_value(rsvp, ['event', 'event_name'], STRING),
                                get_nested_value(rsvp, ['event', 'event_url'], STRING),
                                get_nested_value(rsvp, ['event', 'time'], STRING),
                                get_nested_value(rsvp, ['group', 'group_city'], STRING),
                                get_nested_value(rsvp, ['group', 'group_country'], STRING),
                                get_nested_value(rsvp, ['group', 'group_id'], INT),
                                get_nested_value(rsvp, ['group', 'group_lat'], STRING),
                                get_nested_value(rsvp, ['group', 'group_lon'], STRING),
                                get_nested_value(rsvp, ['group', 'group_name'], STRING),
                                get_nested_value(rsvp, ['group', 'group_urlname'], STRING),
                                get_nested_value(rsvp, ['guests'], INT),
                                get_nested_value(rsvp, ['member', 'member_id'], INT),
                                get_nested_value(rsvp, ['member', 'member_name'], STRING),
                                get_nested_value(rsvp, ['member', 'photo'], STRING),
                                get_nested_value(rsvp, ['mtime'], INT),
                                get_nested_value(rsvp, ['response'], STRING),
                                get_nested_value(rsvp, ['rsvp_id'], INT),
                                get_nested_value(rsvp, ['venue', 'lat'], STRING),
                                get_nested_value(rsvp, ['venue', 'lon'], STRING),
                                get_nested_value(rsvp, ['venue', 'venue_id'], INT),
                                get_nested_value(rsvp, ['venue', 'venue_name'], STRING))
    cursor.commit()
    print("rsvp - inserted to sql server successfully")

def insert_group_topics(cursor, group_topics, rsvp_id):
    '''
    insert data to dbo.group_topics table 
    '''
    
    for topic in group_topics:
        topic_name, urlkey = str(topic.get('topic_name')), str(topic.get('urlkey'))
        cursor.execute("""
                        INSERT INTO dbo.group_topics (id, topic_name, urlkey) 
                        VALUES (?,?,?)""",
                        rsvp_id, topic_name, urlkey)
        cursor.commit()
    print("group_topics - inserted to sql server successfully")


if __name__ == '__main__':
    config = configparser.ConfigParser()
    path = 'conf.ini'
    config.read(path)
    server = config['DEFAULT']['server']
    topic_name = config['DEFAULT']['topic_name']
    conn = sql_server_connector.get_connector(config['SQL_SERVER'])
    cursor = conn.cursor()
    stream_rsvp(topic_name, server, cursor)
