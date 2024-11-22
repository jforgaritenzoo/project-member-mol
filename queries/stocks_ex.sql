SELECT
  st_prdcd as plu,
  st_kodeigr as branch_id,
  CASE
    WHEN floor(st_adjust / 100 *(saldoakhir - pbomi - kphmean)) < 0 THEN 0
    ELSE floor(st_adjust / 100 *(saldoakhir - pbomi - kphmean))
  END as qty,
  null as flag_aktif,
  sysdate as created_at,
  sysdate as updated_at,
  saldoakhir
FROM
  (
    select
      st_prdcd,
      CASE
        WHEN pbomi IS NULL THEN 0
        ELSE pbomi
      END as pbomi,
      CASE
        WHEN kphmean IS NULL THEN 0
        ELSE kphmean
      END as kphmean,
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
                  WHEN aj.st_adjust is null THEN 20
                  ELSE aj.st_adjust
                END as st_adjust,
                CASE
                  WHEN ki.SAT_GRAM is not null THEN floor(sum(st.st_saldoakhir) / ki.SAT_GRAM)
                  ELSE sum(st.st_saldoakhir)
                END as saldoakhir
              from
                tbmaster_stock_interface st
                left join adjust_stocks aj on aj.st_prdcd = st.st_prdcd
                and aj.st_kodeigr = st.st_kodeigr
                left join konversi_item_klikigr ki on pluigr = st.st_prdcd
              where
                st.st_lokasi = '01'
              group by
                st.st_prdcd,
                st.st_kodeigr,
                sat_gram,
                st_adjust
            )
          ) a
          left join (
            select
              thp_prdcd,
              thp_kodeigr,
              pbomi,
              kphmean
            from
              (
                select
                  thp_prdcd,
                  thp_kodeigr,
                  ceil(3 *(sum(thp_avgpb_omi))) as pbomi,
                  ceil(5 *(sum(thp_kph))) as kphmean
                from
                  TBTR_HITUNG_PB_INTERFACE tb
                  LEFT JOIN (
                    select
                      max(thp_periode) as periode,
                      thp_kodeigr as cab
                    from
                      tbtr_hitung_pb_interface
                    group by
                      thp_kodeigr
                  ) a ON tb.THP_PERIODE = a.periode
                GROUP by
                  thp_prdcd,
                  thp_kodeigr
              )
          ) b on a.st_prdcd = b.thp_prdcd
          and a.st_kodeigr = b.thp_kodeigr
      )
  )