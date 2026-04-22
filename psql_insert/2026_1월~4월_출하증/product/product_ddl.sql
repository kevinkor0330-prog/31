-- ============================================================
--  product 테이블  (품목/금형 DATA)
--  Schema : sales
--
--  parent_id = NULL  → 기본 모델  (예: I-3, 비데8000SET)
--  parent_id = 있음  → 배율/버전 변형  (예: I-3(30배), EPO-55#2)
-- ============================================================

CREATE TABLE sales.product (
    id              INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- 자기참조: 배율/버전 변형 모델은 parent_id 로 기본 모델 참조
    parent_id       INT          REFERENCES sales.product(id)
                        ON DELETE SET NULL,

    -- 기본 식별
    code            TEXT         NOT NULL,        -- MODEL(CODE)
    original_name   TEXT,                         -- 원MODEL명 (내부 코드명)
    category        TEXT,                         -- 제품군: SET/BOX/PLT/저발/발포립 등

    -- 제품 스펙 (출하/단가 계산에 사용)
    weight_g        NUMERIC(10, 2),               -- 중량(g)
    ratio           NUMERIC(8,  2),               -- 배율
    grade           TEXT,                         -- 원재료 등급: B240NS 등
    pack_unit       TEXT,                         -- 포장단위
    cavity          INT,                          -- C'TY (캐비티 수)
    cycle_time      INT,                          -- C/T
    efficiency      NUMERIC(5, 2),                -- 능율

    -- 치수
    spec            NUMERIC(10, 2),               -- 규격(mm)
    thickness       NUMERIC(8,  2),               -- 두께(mm)
    t_size_lo       NUMERIC(8,  2),               -- T치수LO
    t_size_up       NUMERIC(8,  2),               -- T치수UP
    b_size_lo       NUMERIC(8,  2),               -- B치수LO
    b_size_up       NUMERIC(8,  2),               -- B치수UP

    -- 외관
    color           TEXT,                         -- 색
    strap           TEXT,                         -- 끈

    -- 기본 단가 (거래처별 다르면 partner_pricelist 참조)
    base_price      NUMERIC(12, 2),               -- 단가

    -- 주거래처 (DATA 시트의 '거래처' 컬럼)
    partner_name    TEXT,                         -- 일단 텍스트로, 나중에 FK로

    -- 금형 정보
    mold_maker      TEXT,                         -- 금형제작처
    mold_location   TEXT,                         -- 금형보관장소
    mold_in_date    DATE,                         -- 최초금형반입일
    mold_out_date   DATE,                         -- 금형영구반출일
    mold_out_to     TEXT,                         -- 반출처
    model_sonata    TEXT,                         -- MODEL(sonata) 내부코드

    -- 기타
    base_ratio      NUMERIC(8, 2),               -- 기준배율
    volume          NUMERIC(12, 2),              -- 체적
    moisture_pct    NUMERIC(5, 2),               -- 함수율(%)
    note            TEXT,                        -- 비고

    -- Audit
    create_date     TIMESTAMPTZ  DEFAULT NOW(),
    write_date      TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_product_parent   ON sales.product(parent_id);
CREATE INDEX idx_product_code     ON sales.product(code);
CREATE INDEX idx_product_category ON sales.product(category);

-- write_date 자동갱신 트리거는 이미 있음 (sales.set_write_date)
CREATE TRIGGER trg_product_write_date
    BEFORE UPDATE ON sales.product
    FOR EACH ROW EXECUTE FUNCTION sales.set_write_date();
