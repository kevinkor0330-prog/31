-- ============================================================
--  stock_move 테이블
--  Schema : sales
--  출하일보 — 날짜, 거래처, 납품처, 모델, 출하수, 차량
-- ============================================================
 
CREATE TABLE sales.stock_move (
    id            INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 
    shipped_date  DATE         NOT NULL,
 
    partner_id    INT          REFERENCES sales.res_partner(id)
                      ON DELETE SET NULL,
    delivery_id   INT          REFERENCES sales.res_partner(id)
                      ON DELETE SET NULL,
 
    product_id    INT          REFERENCES sales.product(id)
                      ON DELETE SET NULL,
    product_code  TEXT,        -- 원본 모델명 보존
 
    shipped_qty   INT,
    vehicle       TEXT,        -- 차량No 원본값
 
    create_date   TIMESTAMPTZ  DEFAULT NOW(),
    write_date    TIMESTAMPTZ  DEFAULT NOW()
);
 
CREATE INDEX idx_sm_date     ON sales.stock_move(shipped_date);
CREATE INDEX idx_sm_partner  ON sales.stock_move(partner_id);
CREATE INDEX idx_sm_product  ON sales.stock_move(product_id);
 
CREATE TRIGGER trg_sm_write_date
    BEFORE UPDATE ON sales.stock_move
    FOR EACH ROW EXECUTE FUNCTION sales.set_write_date();
 