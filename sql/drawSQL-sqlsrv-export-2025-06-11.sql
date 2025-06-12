CREATE TABLE "transactions_fact"(
    "transaction_id (PK)" INT NOT NULL,
    "transaction_date_id (FK)" DATETIME NOT NULL,
    "user_id (FK)" INT NOT NULL,
    "card_id (FK)" INT NOT NULL,
    "merchant_id (FK)" INT NOT NULL,
    "amount" FLOAT(53) NOT NULL,
    "use_chip" VARCHAR(255) NOT NULL,
    "error_type" VARCHAR(255) NOT NULL,
    "zip" VARCHAR(255) NOT NULL
);
CREATE INDEX "index_9ee39213e23acc07c807d8d04c560add" ON
    "transactions_fact"(
        "card_id (FK)",
        "user_id (FK)",
        "merchant_id (FK)",
        "transaction_date_id (FK)"
    );
ALTER TABLE
    "transactions_fact" ADD CONSTRAINT "transactions_fact_transaction_id (pk)_primary" PRIMARY KEY("transaction_id (PK)");
CREATE TABLE "dim_user"(
    "user_id" INT NOT NULL,
    "current_age" INT NOT NULL,
    "retirement_age" INT NOT NULL,
    "birth_year" INT NOT NULL,
    "gender" VARCHAR(255) NOT NULL,
    "address" VARCHAR(255) NOT NULL,
    "latitude" FLOAT(53) NOT NULL,
    "longitude" FLOAT(53) NOT NULL,
    "per_capita_income" FLOAT(53) NOT NULL,
    "yearly_income" FLOAT(53) NOT NULL,
    "total_debt" FLOAT(53) NOT NULL,
    "credit_score" INT NOT NULL,
    "num_credit_cards" INT NOT NULL
);
ALTER TABLE
    "dim_user" ADD CONSTRAINT "dim_user_user_id_primary" PRIMARY KEY("user_id");
CREATE TABLE "dim_card"(
    "card_id" INT NOT NULL,
    "card_brand" INT NOT NULL,
    "card_type" INT NOT NULL,
    "has_chip" BIT NOT NULL,
    "num_cards_issued" INT NOT NULL,
    "credit_limit" FLOAT(53) NOT NULL,
    "acct_open_date" VARCHAR(255) NOT NULL,
    "year_pin_last_changed" INT NOT NULL,
    "card_on_dark_web" BIT NOT NULL,
    "expires" VARCHAR(10) NOT NULL
);
ALTER TABLE
    "dim_card" ADD CONSTRAINT "dim_card_card_id (pk)_primary" PRIMARY KEY("card_id (PK)");
CREATE TABLE "dim_merchant"(
    "merchant_id (PK)" INT NOT NULL,
    "merchant_city" VARCHAR(255) NOT NULL,
    "merchant_state" VARCHAR(255) NOT NULL
);
ALTER TABLE
    "dim_merchant" ADD CONSTRAINT "dim_merchant_merchant_id (pk)_primary" PRIMARY KEY("merchant_id (PK)");
CREATE TABLE "dim_date"(
    "date_id (PK) (Surrogate key (DDMMYYYY)))" VARCHAR(255) NOT NULL,
    "full_date" DATE NOT NULL,
    "year" INT NOT NULL,
    "month" INT NOT NULL,
    "day" INT NOT NULL,
    "weekday" VARCHAR(255) NOT NULL,
    "is_weekend" BIT NOT NULL
);
ALTER TABLE
    "dim_date" ADD CONSTRAINT "dim_date_date_id (pk) (surrogate key (ddmmyyyy)))_primary" PRIMARY KEY(
        "date_id (PK) (Surrogate key (DDMMYYYY)))"
    );
ALTER TABLE
    "transactions_fact" ADD CONSTRAINT "transactions_fact_merchant_id (fk)_foreign" FOREIGN KEY("merchant_id (FK)") REFERENCES "dim_merchant"("merchant_id (PK)");
ALTER TABLE
    "transactions_fact" ADD CONSTRAINT "transactions_fact_card_id (fk)_foreign" FOREIGN KEY("card_id (FK)") REFERENCES "dim_card"("card_id (PK)");
ALTER TABLE
    "transactions_fact" ADD CONSTRAINT "transactions_fact_user_id (fk)_foreign" FOREIGN KEY("user_id (FK)") REFERENCES "dim_user"("user_id");
ALTER TABLE
    "transactions_fact" ADD CONSTRAINT "transactions_fact_transaction_date_id (fk)_foreign" FOREIGN KEY("transaction_date_id (FK)") REFERENCES "dim_date"(
        "date_id (PK) (Surrogate key (DDMMYYYY)))"
    );