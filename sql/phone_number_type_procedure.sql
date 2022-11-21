CREATE OR REPLACE PROCEDURE sp_phone_number_type()
    language plpgsql
AS
$$
DECLARE
    cur_user         varchar   := (SELECT CURRENT_USER);
    DECLARE cur_time timestamp := (Select current_timestamp);
Begin

    INSERT INTO phone_number_type
    (type,
     created,
     created_by,
     updated,
     updated_by)
        (VALUES ('phone_home', cur_time, cur_user, cur_time, cur_user))
    ON CONFLICT ON CONSTRAINT phone_number_type_pkey
        DO UPDATE SET updated    = cur_time,
                      updated_by = cur_user;

    INSERT INTO phone_number_type
    (type,
     created,
     created_by,
     updated,
     updated_by)
        (VALUES ('phone_cell', cur_time, cur_user, cur_time, cur_user))
    ON CONFLICT ON CONSTRAINT phone_number_type_pkey
        DO UPDATE SET updated    = cur_time,
                      updated_by = cur_user;

end
$$;