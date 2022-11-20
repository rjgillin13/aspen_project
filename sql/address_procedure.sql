CREATE OR REPLACE PROCEDURE sp_address() language plpgsql
AS $$
DECLARE cur_user varchar := (SELECT CURRENT_USER);
DECLARE cur_time timestamp := (Select current_timestamp);
Begin

INSERT INTO address
(
    role_profile_id,
    street,
    city,
    state,
    zip_code,
    created,
    created_by,
    updated,
    updated_by
)
    (Select
         rp.role_profile_id,
         t2.street,
         t2.city,
         t2.state,
         t2.zip_code,
         cur_time,
         cur_user,
         cur_time,
         cur_user
     FROM
         (Select
              t1.user_id,
              rpt.role_profile_type_id,
              t1.street,t1.city,t1.state,t1.zip_code
          FROM
              (Select
                   b.street,b.city,b.state,b.zip_code,srp.role_profile,up.user_id
               FROM stg_borrower b
                        LEFT JOIN user_profile up
                                  ON up.borrower_id = b.borrower_id
                        LEFT JOIN stg_role_profile srp
                                  ON srp.borrower_id = b.borrower_id) t1
                  LEFT JOIN role_profile_type rpt
                            ON t1.role_profile = rpt.type) t2
             LEFT JOIN role_profile rp
                       on rp.user_id = t2.user_id
                           AND rp.role_profile_type_id = t2.role_profile_type_id)
    ON Conflict ON CONSTRAINT address_pkey
    DO UPDATE SET updated = cur_time,
           updated_by = cur_user;

end
$$;