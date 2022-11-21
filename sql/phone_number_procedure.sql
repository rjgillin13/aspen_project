CREATE OR REPLACE PROCEDURE sp_phone_number()
    language plpgsql
AS
$$
DECLARE
    cur_user         varchar   := (SELECT CURRENT_USER);
    DECLARE cur_time timestamp := (Select current_timestamp);
Begin

    INSERT INTO phone_number
    (role_profile_id,
     phone_number_type_id,
     value,
     created,
     created_by,
     updated,
     updated_by)
        (Select rp.role_profile_id,
                pnt.phone_number_type_id,
                t2.value,
                cur_time,
                cur_user,
                cur_time,
                cur_user
         FROM (Select t1.user_id,
                      rpt.role_profile_type_id,
                      t1.value,
                      t1.type
               FROM (Select b.value,
                            b.type,
                            srp.role_profile,
                            up.user_id
                     FROM (Select borrower_id, phone_home as value, 'phone_home' as type
                           FROM stg_borrower
                           UNION
                           Select borrower_id, phone_cell as value, 'phone_cell' as type
                           FROM stg_borrower) b
                              LEFT JOIN user_profile up
                                        ON up.borrower_id = b.borrower_id
                              LEFT JOIN stg_role_profile srp
                                        ON srp.borrower_id = b.borrower_id) t1
                        LEFT JOIN role_profile_type rpt
                                  ON t1.role_profile = rpt.type) t2
                  LEFT JOIN role_profile rp
                            on rp.user_id = t2.user_id
                                AND rp.role_profile_type_id = t2.role_profile_type_id
                  LEFT JOIN phone_number_type pnt
                            on pnt.type = t2.type)
    ON Conflict ON CONSTRAINT phone_number_pkey
        DO UPDATE SET updated    = cur_time,
                      updated_by = cur_user;

end
$$;