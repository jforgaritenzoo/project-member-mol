select
    st_prdcd as plu,
    st_kodeigr as branch_id,
    CASE
        WHEN floor(st_adjust / 100 *(saldoakhir)) < 0 THEN 0
        ELSE floor(
            st_adjust / 100 *(saldoakhir)
        )
    END as qty,
    null as flag_aktif,
    sysdate as created_at,
    sysdate as updated_at,
    saldoakhir
from
    (
        select
            st_prdcd,
            saldoakhir,
            st_kodeigr,
            st_adjust
        from
            (
                select
                    *
                from
                    (
                        (
                            select
                                st.st_prdcd as st_prdcd,
                                st.st_kodeigr as st_kodeigr,
                                CASE
                                    WHEN st_adjust is null THEN 20
                                    ELSE st_adjust
                                END as st_adjust,
                                CASE
                                    WHEN SAT_GRAM is not null THEN floor(
                                        sum(st.st_saldoakhir) / SAT_GRAM
                                    )
                                    ELSE sum(st.st_saldoakhir)
                                END as saldoakhir
                            from
                                tbmaster_stock_interface st
                                left join adjust_stocks aj on aj.st_prdcd = st.st_prdcd
                                and aj.st_kodeigr = st.st_kodeigr
                                left join konversi_item_klikigr on pluigr = st.st_prdcd
                            where
                                st.st_lokasi = '01'
                            group by
                                st.st_prdcd,
                                st.st_kodeigr,
                                sat_gram,
                                st_adjust
                        )
                    ) a
            )
    )
where
    st_kodeigr not in (
        select
            spi_kodespi
        from
            tbmaster_spi_interface
    )