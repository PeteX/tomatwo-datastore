drop schema datastoretest cascade;
create schema datastoretest;
set search_path to datastoretest;
alter user datastoretest set search_path to datastoretest;

create table account (
    id text primary key,
    data jsonb not null
);

create table contact (
    id text primary key,
    data jsonb not null
);

create index account_favourite_number on account (((data ->> 'FavouriteNumber')::integer));

grant usage on schema datastoretest to datastoretest;
grant select, insert, update, delete on all tables in schema datastoretest to datastoretest;
