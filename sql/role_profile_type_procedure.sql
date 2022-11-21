CREATE OR REPLACE PROCEDURE sp_role_profile_type()
    language plpgsql
AS
$$
DECLARE
    cur_user         varchar   := (SELECT CURRENT_USER);
    DECLARE cur_time timestamp := (Select current_timestamp);
Begin

    INSERT INTO role_profile_type
    (type,
     created,
     created_by,
     updated,
     updated_by)
        (SELECT DISTINCT role_profile,
                         cur_time,
                         cur_user,
                         cur_time,
                         cur_user
         FROM stg_role_profile)
    ON Conflict ON CONSTRAINT role_profile_type_pkey
        DO UPDATE SET updated    = cur_time,
                      updated_by = cur_user;

end
$$;