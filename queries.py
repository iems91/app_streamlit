vendas= """
        select
            v.CODUSUR, trunc(v.dtsaida) DATA, sum(v.vlvenda) VALOR, sum(v.vlcustofin) CUSTO
        from 
            view_vendas_resumo_faturamento v
        where 
            condvenda = 1
            and dtcancel is null
        group by 
            v.codusur, v.dtsaida
    """
data_rca= """
        select
            CODUSUR, trunc(dtsaida) DATA
        from 
            view_vendas_resumo_faturamento
    """
devol= """
        select
            d.CODUSUR, trunc(d.dtent) DATA, sum(d.vldevolucao) VALOR, sum(d.vlcustofin) CUSTO
        from 
            view_devol_resumo_faturamento d
        where 
            condvenda = 1
            and dtcancel is null
        group by 
            d.codusur, d.dtent
    """
devol_avulsa="""
        select
           a.CODUSUR, trunc(a.dtent) DATA, sum(a.vldevolucao) VALOR, sum(a.vlcustofin) CUSTO
        from 
            view_devol_resumo_faturavulsa a
        group by 
            a.codusur, a.dtent 
    """
vendas_completa= """
        SELECT 
              dtsaida DATA
            , codusur
            , codfilial
            , numped
            , codprod
            , codprodprinc
            , codcli
            , codcliprinc
            , ramo_ativ
            , estent
            , municent
            , tipofj
            , cobranca
            , entrega
            , valor_total_item
            , vldesconto
            , prazomedio
            , numseq
        FROM 
            saidas1
        WHERE 
            OPERACAO = 'SAÍDA' 
            AND CONDVENDA = 'VENDA' 
            AND DTCANCEL IS NULL

    """

duck_faturamento = """
    select CODUSUR, VALOR_TOTAL_ITEM, DATA
    FROM vendas_completa
    WHERE 1=1
"""

graf_linha_faturamento = """
        SELECT 
            DATA::DATE AS DATA, 
            SUM(VALOR_TOTAL_ITEM) AS FATURAMENTO_DIARIO
        FROM vendas_completa
        GROUP BY DATA::DATE
        ORDER BY DATA::DATE;
        """