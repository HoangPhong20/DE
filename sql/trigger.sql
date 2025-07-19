CREATE TABLE users_log_before (
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) default CURRENT_TIMESTAMP(3)
);
CREATE TABLE users_log_after (
    user_id BIGINT,
    login VARCHAR(255),
    gravatar_id VARCHAR(255),
    url VARCHAR(255),
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) default CURRENT_TIMESTAMP(3)
);

DELIMITER //

CREATE TRIGGER before_ins_users
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id,login,gravatar_id,url,avatar_url,state)
    VALUES (NEW.user_id,NEW.login,NEW.gravatar_id,NEW.url,NEW.avatar_url,"INSERT");
END;
//

CREATE TRIGGER before_update_users
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, 'UPDATE');
END;
//

CREATE TRIGGER before_delete_users
BEFORE DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_before(user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, 'DELETE');
END;
//

CREATE TRIGGER after_ins_users
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id,login,gravatar_id,url,avatar_url,state)
    VALUES (NEW.user_id,NEW.login,NEW.gravatar_id,NEW.url,NEW.avatar_url,"INSERT");
END;
//

CREATE TRIGGER after_update_users
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, 'UPDATE');
END;

CREATE TRIGGER after_delete_users
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO users_log_after(user_id, login, gravatar_id, url, avatar_url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, 'DELETE');
END;
//

DELIMITER ;

-- insert data
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (1, 'user1', 'gravatar1', 'https://avatar.com/user1', 'https://api.user1.com');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (2, 'user2', 'gravatar2', 'https://avatar.com/user2', 'https://api.user2.com');
INSERT INTO users (user_id, login, gravatar_id, avatar_url) VALUES (3, 'user3', 'gravatar3', 'https://avatar.com/user3');
INSERT INTO users (user_id, login, gravatar_id) VALUES (4, 'user4', 'gravatar4');
INSERT INTO users (user_id, login, avatar_url, url) VALUES (5, 'user5', 'https://avatar.com/user5', 'https://api.user5.com');
INSERT INTO users (user_id, login) VALUES (6, 'user6');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (7, 'user7', 'gravatar7', 'https://avatar.com/user7', 'https://api.user7.com');
INSERT INTO users (user_id, login, avatar_url) VALUES (8, 'user8', 'https://avatar.com/user8');
INSERT INTO users (user_id, login, gravatar_id, url) VALUES (9, 'user9', 'gravatar9', 'https://api.user9.com');
INSERT INTO users (user_id, login, gravatar_id, avatar_url, url) VALUES (10, 'user10', 'gravatar10', 'https://avatar.com/user10', 'https://api.user10.com');





-- update data
UPDATE users SET login = 'new_user1' WHERE user_id = 1;
UPDATE users SET avatar_url = 'https://newavatar.com/user2' WHERE user_id = 2;
UPDATE users SET gravatar_id = 'new_gravatar3', url = 'https://newapi.user3.com' WHERE user_id = 3;
UPDATE users SET login = 'updated_user4', avatar_url = 'https://avatar.com/updated4' WHERE user_id = 4;
UPDATE users SET url = 'https://api.updated5.com' WHERE login = 'user5';
UPDATE users SET gravatar_id = NULL WHERE user_id = 6;
UPDATE users SET login = 'user7_updated', avatar_url = 'https://avatar.com/user7new' WHERE user_id = 7;
UPDATE users SET url = NULL, gravatar_id = 'gravatar8new' WHERE user_id = 8;
UPDATE users SET login = 'user9_new', avatar_url = 'https://avatar.com/user9new', url = 'https://api.user9new.com' WHERE user_id = 9;
UPDATE users SET gravatar_id = 'gravatar10_updated' WHERE login = 'user10';

-- delete data
DELETE FROM users WHERE user_id = 1;
DELETE FROM users WHERE login = 'user2';
DELETE FROM users WHERE gravatar_id = 'gravatar3';
