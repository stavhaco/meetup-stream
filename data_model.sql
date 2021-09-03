USE themis;

CREATE TABLE group_topics (
    id int,
    topic_name varchar(255),
    urlkey varchar(255)
);

CREATE TABLE rsvp (
        event_event_id varchar(255),
        event_event_name varchar(255),
        event_event_url varchar(255),
        event_time bigint,
        group_group_city varchar(255),
        group_group_country varchar(255),
        group_group_id bigint,
        group_group_lat varchar(255),
        group_group_lon varchar(255),
        group_group_name varchar(255),
        group_group_urlname varchar(255),
        guests bigint,
        member_member_id bigint,
        member_member_name varchar(255),
        member_photo varchar(255),
        mtime bigint,
        response varchar(255),
        rsvp_id bigint,
        venue_lat varchar(255),
        venue_lon varchar(255),
        venue_venue_id varchar(255),
        venue_venue_name varchar(255)
);
