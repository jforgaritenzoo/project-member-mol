SELECT
    prd_kodeigr AS kode_igr,
    prd_prdcd AS prdcd,
    prd_plumcg AS plu_mcg,
    prd_deskripsipanjang AS long_description,
    prd_deskripsipendek AS short_description,
    prd_minjual AS min_jual,
    prd_unit AS unit,
    prd_frac AS frac,
    prd_hrgjual AS hrg_jual,
    NVL(prd_kodetag, '') AS kode_tag,
    prd_kodedivisi AS kode_division,
    prd_kodedepartement AS kode_department,
    prd_kodekategoribarang AS kode_category,
    prd_brg_merk AS merk,
    flagpromomd,
    null as flagpromomkt,
    prmd_hrgjual,
    width,
    LENGTH,
    height,
    weight,
    brg_merk,
    brg_flavor,
    brg_ukuran,
    brg_brutoctn,
    brg_brutopcs,
    flag_klik,
    prdcd_stock
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    ROW_NUMBER () OVER (
                        PARTITION BY SUBSTR (PRD_PRDCD, 1, 6),
                        PRD_KODEIGR
                        ORDER BY
                            CASE
                                WHEN SUBSTR (PRD_PRDCD, 7, 1) = '1' THEN SUBSTR (PRD_PRDCD, 1, 6) || '4'
                                ELSE PRD_PRDCD
                            END DESC
                    ) ROWN,
                    PRD_PRDCD,
                    prd_plumcg,
                    PRD_KODEIGR,
                    PRD_KODETAG,
                    PRD_DESKRIPSIPANJANG,
                    prd_hrgjual,
                    prd_minjual,
                    PRD_UNIT,
                    PRD_BRG_MERK,
                    PRD_FRAC,
                    PRD_FLAGBKP1,
                    PRD_FLAGBKP2,
                    NULL URL_GBR_PROD,
                    NULL BERAT,
                    PRD_DESKRIPSIPENDEK,
                    prd_kodedivisi,
                    PRD_KODEDEPARTEMENT,
                    prd_kodekategoribarang,
                    prd_recordid,
                    prd_flaghbv,
                    prd_avgcost,
                    prd_openprice,
                    PRD_DIMENSILEBAR AS width,
                    PRD_DIMENSIPANJANG AS LENGTH,
                    PRD_DIMENSITINGGI AS height,
                    PRD_BRG_SIZE AS weight,
                    prd_flagobi as flag_klik,
                    prd_flagigr,
                    prd_flag_aktivasi,
                    SUBSTR(prd_prdcd, 0, 6) || 0 as prdcd_stock
                FROM
                    TBMASTER_PRODMAST_INTERFACE
                WHERE
                    (
                        nvl(PRD_KODETAG, ' ') NOT IN (
                            'A',
                            'H',
                            'N',
                            'O',
                            'X',
                            'C',
                            'U',
                            'Z'
                        )
                    )
                    and prd_flag_aktivasi is null
            )
            LEFT JOIN (
                SELECT
                    DISTINCT prd_prdcd plu,
                    prmd_hrgjual,
                    1 AS flagpromomd,
                    prmd_kodeigr
                FROM
                    tbmaster_prodmast_interface
                    JOIN tbtr_promomd_interface ON prmd_prdcd = prd_prdcd
                    AND PRMD_KODEIGR = PRD_KODEIGR
                WHERE
                    TRUNC (SYSDATE) <= TRUNC (prmd_tglakhir)
            ) ON prd_prdcd = plu
            AND prd_kodeigr = prmd_kodeigr
            LEFT JOIN (
                SELECT
                    DISTINCT brg_prdcd,
                    brg_merk,
                    brg_flavor,
                    brg_ukuran,
                    brg_brutoctn,
                    brg_brutopcs,
                    brg_kodeigr
                FROM
                    tbmaster_barang_INTERFACE
                WHERE
                    brg_merk NOT LIKE '*%'
            ) ON CONCAT (
                SUBSTR (PRD_PRDCD, 1, 6),
                0
            ) = brg_prdcd
            AND BRG_KODEIGR = PRD_KODEIGR
        WHERE
            NVL (prd_openprice, 'N') <> 'Y'
            AND NVL(prd_avgcost, 0) > 0
            AND NVL (prd_flaghbv, 'N') <> 'Y'
            AND prd_recordid IS NULL
            AND flag_klik = 'Y'
            and prd_flag_aktivasi is null -- and prd_kodeigr ='18'
        ORDER BY
            PRD_PRDCD
    )
WHERE
    (
        PRD_KODEDIVISI <> '4'
        OR flag_klik = 'Y'
    )
    AND (
        PRD_UNIT <> 'KG'
        OR substr(prd_prdcd, 1, 6) in(
            select
                substr(PLUIGR, 1, 6)
            from
                KONVERSI_ITEM_KLIKIGR
        )
    ) --   AND (PRD_UNIT <> 'KG' OR flag_klik = 'Y')
    AND (
        PRD_KODEDIVISI != '3'
        OR PRD_KODEDEPARTEMENT != '42'
        OR PRD_KODEKATEGORIBARANG != '02'
    )
    AND ROWN = 1