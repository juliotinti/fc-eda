INSERT INTO clients (id, name, email, created_at) VALUES
    ('00000000-0000-4000-8000-000000000001', 'Ana', 'ana@example.com', NOW()),
    ('00000000-0000-4000-8000-000000000002', 'Bruno', 'bruno@example.com', NOW());

INSERT INTO accounts (id, client_id, balance, created_at) VALUES
    ('f8df753c-3b58-43aa-8016-12aaa4f1ea3e', '00000000-0000-4000-8000-000000000001', 1000, NOW()),
    ('0216ea38-524f-4e85-8743-d484a8f7538e', '00000000-0000-4000-8000-000000000002', 250, NOW());
