SELECT
    prd_kodeigr as kode_igr,
    prd_prdcd as prdcd,
    prd_plumcg as plu_mcg,
    prd_deskripsipanjang as long_description,
    prd_deskripsipendek as short_description,
    prd_minjual as min_jual,
    prd_unit as unit,
    Prd_frac as frac,
    prd_hrgjual as hrg_jual,
    prd_kodetag as kode_tag,
    prd_kodedivisi as kode_division,
    prd_kodedepartement as kode_department,
    prd_kodekategoribarang as kode_category,
    prd_brg_merk as merk,
    null as flagpromomd,
    null as flagpromomkt,
    null as prmd_hrgjual,
    prd_dimensilebar as width,
    prd_dimensipanjang as length,
    prd_dimensitinggi as height,
    prd_flagobi as flag_klik,
    prd_flagigr as flag_igr,
    prd_flagomi as flag_omi,
    prd_flagidm as flag_idm
FROM
    tbmaster_prodmast_interface
WHERE
    nvl(prd_openprice, 'N') <> 'Y'
    and nvl(prd_flaghbv, 'N') <> 'Y'
    and prd_recordid is null