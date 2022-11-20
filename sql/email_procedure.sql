CREATE OR REPLACE PROCEDURE sp_email() language plpgsql
AS $$
DECLARE cur_user varchar := (SELECT CURRENT_USER);
DECLARE cur_time timestamp := (Select current_timestamp);
Begin

INSERT INTO email
(
    role_profile_id,
    email_type_id,
    value,
    created,
    created_by,
    updated,
    updated_by
)
    (Select
         rp.role_profile_id,
         et.email_type_id,
         t2.email,
         cur_time,
         cur_user,
         cur_time,
         cur_user
     FROM
         (Select
              t1.user_id,
              rpt.role_profile_type_id,
              t1.email,
              substring(trim(t1.email) FROM '[@](.*)[.]') as email_type
          FROM
              (Select b.borrower_id,srp.role_profile,up.user_id,email
               FROM stg_borrower b
                        LEFT JOIN user_profile up
                                  ON up.borrower_id = b.borrower_id
                        LEFT JOIN stg_role_profile srp
                                  ON srp.borrower_id = b.borrower_id) t1
                  LEFT JOIN role_profile_type rpt
                            ON t1.role_profile = rpt.type) t2
             LEFT JOIN role_profile rp
                       on rp.user_id = t2.user_id
                           AND rp.role_profile_type_id = t2.role_profile_type_id
             LEFT JOIN email_type et
                       on et.type = t2.email_type)
    ON CONFLICT ON CONSTRAINT email_pkey
    DO UPDATE SET updated = cur_time,
           updated_by = cur_user;

end
$$;