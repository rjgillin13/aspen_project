CREATE OR REPLACE PROCEDURE sp_role_profile()
    language plpgsql
AS
$$
DECLARE
    cur_user         varchar   := (SELECT CURRENT_USER);
    DECLARE cur_time timestamp := (Select current_timestamp);
Begin

    INSERT INTO role_profile
    (user_id,
     role_profile_type_id,
     created,
     created_by,
     updated,
     updated_by)
        (Select DISTINCT t1.user_id,
                         rpt.role_profile_type_id,
                         cur_time,
                         cur_user,
                         cur_time,
                         cur_user
         FROM (Select up.user_id,
                      srp.role_profile
               FROM stg_borrower b
                        LEFT JOIN user_profile up
                                  ON
                                      up.borrower_id = b.borrower_id
                        LEFT JOIN stg_role_profile srp
                                  ON srp.borrower_id = b.borrower_id) t1
                  LEFT JOIN role_profile_type rpt
                            ON rpt.type = t1.role_profile)
    ON Conflict ON CONSTRAINT role_profile_pkey
        DO UPDATE SET updated    = cur_time,
                      updated_by = cur_user;

end
$$;