CREATE OR REPLACE PROCEDURE sp_user_profile() language plpgsql
AS $$
DECLARE cur_user varchar := (SELECT CURRENT_USER);
DECLARE cur_time timestamp := (Select current_timestamp);
Begin

INSERT INTO user_profile
(
    borrower_id,
    first_name,
    last_name,
    created_date,
    created_by,
    updated_date,
    updated_by
)
    (
        SELECT
            DISTINCT
            borrower_id,
            substring(trim(full_name) FROM '^([^ ]+)') as first_name,
            substring(trim(full_name) FROM '([^ ]+)$') as last_name,
            cur_time,
            cur_user,
            cur_time,
            cur_user
        FROM
            stg_borrower)
    ON CONFLICT ON CONSTRAINT user_profile_pkey
    DO UPDATE SET updated_date = cur_time,
           updated_by = cur_user;

end
$$;