create schema ack;

create table ack.item (
  id uuid primary key
  , metadata json -- { ..., lastSeen }
);