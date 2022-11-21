CREATE OR REPLACE PROCEDURE sp_email_type()
    language plpgsql
AS
$$
DECLARE
    cur_user         varchar   := (SELECT CURRENT_USER);
    DECLARE cur_time timestamp := (Select current_timestamp);
Begin

    INSERT INTO email_type
    (type,
     created,
     created_by,
     updated,
     updated_by)
        (SELECT DISTINCT substring(trim(email) FROM '[@](.*)[.]') as type,
                         cur_time,
                         cur_user,
                         cur_time,
                         cur_user
         FROM stg_borrower)
    ON CONFLICT ON CONSTRAINT email_type_pkey
        DO UPDATE SET updated    = cur_time,
                      updated_by = cur_user;

end
$$;