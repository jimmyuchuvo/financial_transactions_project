
CREATE CLUSTERED COLUMNSTORE INDEX idx_CS_fact_transactions
ON financial.fact_transactions;


ALTER TABLE financial.dim_card
ADD CONSTRAINT PK_dim_card_card_id PRIMARY KEY (card_id);

ALTER TABLE financial.dim_date
ADD CONSTRAINT PK_dim_date_date_key PRIMARY KEY (date_key);

ALTER TABLE financial.dim_merchant
ADD CONSTRAINT PK_dim_merchant_merchant_key PRIMARY KEY (merchant_key);

ALTER TABLE financial.dim_users
ADD CONSTRAINT PK_dim_users_user_id PRIMARY KEY (user_id);




