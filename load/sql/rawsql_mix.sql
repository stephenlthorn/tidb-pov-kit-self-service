SELECT id, balance, status FROM accounts WHERE id = FLOOR(1 + RAND() * 750000);
SELECT id, email, name, status FROM users WHERE id = FLOOR(1 + RAND() * 500000);
SELECT id, amount, status, created_at FROM transactions WHERE account_id = FLOOR(1 + RAND() * 750000) ORDER BY created_at DESC LIMIT 20;
UPDATE accounts SET balance = balance + 1 WHERE id = FLOOR(1 + RAND() * 750000);
UPDATE accounts SET balance = balance - 1 WHERE id = FLOOR(1 + RAND() * 750000) AND balance >= 1;
