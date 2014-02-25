CREATE TABLE IF NOT EXISTS user(id uuid PRIMARY KEY, name varchar, email varchar, password varchar, age int);
CREATE TABLE IF NOT EXISTS tracks (id timeuuid PRIMARY KEY, title varchar, release timestamp, duration float, tags set<varchar>);
CREATE TABLE IF NOT EXISTS user_click_stream (user_id uuid, when timestamp, url varchar, PRIMARY KEY(user_id, when));
CREATE TABLE IF NOT EXISTS track_likes (user_id uuid, track_id timeuuid, PRIMARY KEY (user_id, track_id)) WITH COMPACT STORAGE;