package inflacao

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"crawlers/runners/selic"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

type Data struct {
	Referencia string `json:"referencia" csv:"referencia"`
	Ano        string `json:"ano" csv:"ano"`
	Mes        string `json:"mes" csv:"mes"`
	AnoMes     int64  `json:"ano_mes" csv:"ano_mes"`

	IPCAVariacao            float64 `json:"ipca_variacao" csv:"ipca_variacao"`
	IPCAVariacaoAlimentacao float64 `json:"ipca_variacao_alimentacao" csv:"ipca_variacao_alimentacao"`
	IPCAVariacaoHabitacao   float64 `json:"ipca_variacao_habitacao" csv:"ipca_variacao_habitacao"`
	IPCAVariacaoResidencia  float64 `json:"ipca_variacao_artigos_residencia" csv:"ipca_variacao_artigos_residencia"`
	IPCAVariacaoVestuario   float64 `json:"ipca_variacao_vestuario" csv:"ipca_variacao_vestuario"`
	IPCAVariacaoTransporte  float64 `json:"ipca_variacao_transporte" csv:"ipca_variacao_transporte"`
	IPCAVariacaoSaude       float64 `json:"ipca_variacao_saude" csv:"ipca_variacao_saude"`
	IPCAVariacaoPessoais    float64 `json:"ipca_variacao_despesas_pessoais" csv:"ipca_variacao_despesas_pessoais"`
	IPCAVariacaoEducacao    float64 `json:"ipca_variacao_educacao" csv:"ipca_variacao_educacao"`
	IPCAVariacaoComunicacao float64 `json:"ipca_variacao_comunicacao" csv:"ipca_variacao_comunicacao"`

	IPCAAcumuladoAno            float64 `json:"ipca_acumulado_ano" csv:"ipca_acumulado_ano"`
	IPCAAcumuladoAnoAlimentacao float64 `json:"ipca_acumulado_ano_alimentacao" csv:"ipca_acumulado_ano_alimentacao"`
	IPCAAcumuladoAnoHabitacao   float64 `json:"ipca_acumulado_ano_habitacao" csv:"ipca_acumulado_ano_habitacao"`
	IPCAAcumuladoAnoResidencia  float64 `json:"ipca_acumulado_ano_artigos_residencia" csv:"ipca_acumulado_ano_artigos_residencia"`
	IPCAAcumuladoAnoVestuario   float64 `json:"ipca_acumulado_ano_vestuario" csv:"ipca_acumulado_ano_vestuario"`
	IPCAAcumuladoAnoTransporte  float64 `json:"ipca_acumulado_ano_transporte" csv:"ipca_acumulado_ano_transporte"`
	IPCAAcumuladoAnoSaude       float64 `json:"ipca_acumulado_ano_saude" csv:"ipca_acumulado_ano_saude"`
	IPCAAcumuladoAnoPessoais    float64 `json:"ipca_acumulado_ano_despesas_pessoais" csv:"ipca_acumulado_ano_despesas_pessoais"`
	IPCAAcumuladoAnoEducacao    float64 `json:"ipca_acumulado_ano_educacao" csv:"ipca_acumulado_ano_educacao"`
	IPCAAcumuladoAnoComunicacao float64 `json:"ipca_acumulado_ano_comunicacao" csv:"ipca_acumulado_ano_comunicacao"`

	IPCAAcumulado12Meses            float64 `json:"ipca_acumulado_doze_meses" csv:"ipca_acumulado_doze_meses"`
	IPCAAcumulado12MesesAlimentacao float64 `json:"ipca_acumulado_doze_meses_alimentacao" csv:"ipca_acumulado_doze_meses_alimentacao"`
	IPCAAcumulado12MesesHabitacao   float64 `json:"ipca_acumulado_doze_meses_habitacao" csv:"ipca_acumulado_doze_meses_habitacao"`
	IPCAAcumulado12MesesResidencia  float64 `json:"ipca_acumulado_doze_meses_artigos_residencia" csv:"ipca_acumulado_doze_meses_artigos_residencia"`
	IPCAAcumulado12MesesVestuario   float64 `json:"ipca_acumulado_doze_meses_vestuario" csv:"ipca_acumulado_doze_meses_vestuario"`
	IPCAAcumulado12MesesTransporte  float64 `json:"ipca_acumulado_doze_meses_transporte" csv:"ipca_acumulado_doze_meses_transporte"`
	IPCAAcumulado12MesesSaude       float64 `json:"ipca_acumulado_doze_meses_saude" csv:"ipca_acumulado_doze_meses_saude"`
	IPCAAcumulado12MesesPessoais    float64 `json:"ipca_acumulado_doze_meses_despesas_pessoais" csv:"ipca_acumulado_doze_meses_despesas_pessoais"`
	IPCAAcumulado12MesesEducacao    float64 `json:"ipca_acumulado_doze_meses_educacao" csv:"ipca_acumulado_doze_meses_educacao"`
	IPCAAcumulado12MesesComunicacao float64 `json:"ipca_acumulado_doze_meses_comunicacao" csv:"ipca_acumulado_doze_meses_comunicacao"`

	IPCA15Variacao         float64 `json:"ipca15_variacao" csv:"ipca15_variacao"`
	IPCA15AcumuladoAno     float64 `json:"ipca15_acumulado_ano" csv:"ipca15_acumulado_ano"`
	IPCA15Acumulado12Meses float64 `json:"ipca15_acumulado_doze_meses" csv:"ipca15_acumulado_doze_meses"`
	INPCVariacao           float64 `json:"inpc_variacao" csv:"inpc_variacao"`
	INPCAcumuladoAno       float64 `json:"inpc_acumulado_ano" csv:"inpc_acumulado_ano"`
	INPCAcumulado12Meses   float64 `json:"inpc_acumulado_doze_meses" csv:"inpc_acumulado_doze_meses"`
	IPAVariacao            float64 `json:"ipa_variacao" csv:"ipa_variacao"`
	IPAAcumuladoAno        float64 `json:"ipa_acumulado_ano" csv:"ipa_acumulado_ano"`
	// IPCVariacao            float64 `json:"ipc_fipe_variacao" csv:"ipc_fipe_variacao"`
	// IPCAcumuladoAno        float64 `json:"ipc_fipe_acumulado_ano" csv:"ipc_fipe_acumulado_ano"`
	INCCVariacao     float64 `json:"incc_variacao" csv:"incc_variacao"`
	INCCAcumuladoAno float64 `json:"incc_acumulado_ano" csv:"incc_acumulado_ano"`
	INCCMVariacao    float64 `json:"incc_m_variacao" csv:"incc_m_variacao"`
	INCCMcumuladoAno float64 `json:"incc_m_acumulado_ano" csv:"incc_m_acumulado_ano"`
	SelicMeta        float64 `json:"selic_meta" csv:"selic_meta"`
	SelicAno         float64 `json:"selic_ano" csv:"selic_ano"`
	JurosReais       float64 `json:"juros_reais" csv:"juros_reais"`
	SalarioMinimo    float64 `json:"salario_minimo" csv:"salario_minimo"`
	PibValor         float64 `json:"pib_valor" csv:"pib_valor"`
	DividaPublica    float64 `json:"divida_publica_pib" csv:"divida_publica_pib"`
	ConsolidacaoAno  bool    `json:"consolidado_ano" csv:"consolidado_ano"`
}

type Inflacao struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []Data    `json:"data"`
}

func RunnerConsolidacao() {
	l := logger.Instance()

	time.Sleep(20 * time.Second)

	runnerName := "Inflação - Consolidacao"

	unidadeMedida := "%"
	fonte := "https://servicodados.ibge.gov.br"

	ipcaFile := "./data/inflacao/ipca.json"
	ipcaDetalhadoFile := "./data/inflacao/ipca_detalhado.json"
	ipcaDetalhadoAnoFile := "./data/inflacao/ipca_detalhado_ano.json"
	ipcaDetalhado12MesesFile := "./data/inflacao/ipca_detalhado_12_meses.json"
	ipca15File := "./data/inflacao/ipca15.json"
	inpcFile := "./data/inflacao/inpc.json"
	ipaFile := "./data/inflacao/ipa.json"
	// ipcFile := "./data/inflacao/ipc-fipe.json"
	inccFile := "./data/inflacao/incc.json"
	inccMFile := "./data/inflacao/incc-m.json"
	salarioMinimoFile := "./data/inflacao/salario_minimo.json"
	selicFile := "./data/selic/selic-meta.json"
	selicAnoFile := "./data/selic/selic-percentual-ano.json"
	dividaPublicaFile := "./data/inflacao/divida_publica.json"
	pibValorFile := "./data/inflacao/pib_valor.json"

	file_path := "./data/inflacao/inflacao.json"
	fileNameOutputCSV := "./data/inflacao/inflacao.csv"

	s3KeyCSV := "inflacao/inflacao.csv"
	s3KeyJSON := "inflacao/inflacao.json"

	consolidado := make(map[string]Data)

	inflacao := Inflacao{}

	now := time.Now()
	inflacao.Atualizacao = now
	inflacao.Fonte = fonte
	inflacao.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner Consolidar os Datasets")

	IPCA := IPCA{}
	fileIPCA, err := ioutil.ReadFile(ipcaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCA), &IPCA)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("converter para struct")
	}

	IPCADetalhadoD := IPCADetalhado{}
	fileIPCADetalhado, err := ioutil.ReadFile(ipcaDetalhadoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCADetalhado), &IPCADetalhadoD)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("converter para struct")
	}

	IPCADetalhadoAno := IPCADetalhado{}
	fileIPCADetalhadoAno, err := ioutil.ReadFile(ipcaDetalhadoAnoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCADetalhadoAno), &IPCADetalhadoAno)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("converter para struct")
	}

	IPCADetalhado12Meses := IPCADetalhado{}
	fileIPCADetalhado12Meses, err := ioutil.ReadFile(ipcaDetalhado12MesesFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCADetalhado12Meses), &IPCADetalhado12Meses)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipcaFile).
			Msg("converter para struct")
	}

	IPCA15 := IPCA15{}
	fileIPCA15, err := ioutil.ReadFile(ipca15File)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipca15File).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPCA15), &IPCA15)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipca15File).
			Msg("converter para struct")
	}

	INPC := INPC{}
	fileINPC, err := ioutil.ReadFile(inpcFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inpcFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileINPC), &INPC)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inpcFile).
			Msg("converter para struct")
	}

	IPA := IPA{}
	fileIPA, err := ioutil.ReadFile(ipaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIPA), &IPA)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ipaFile).
			Msg("converter para struct")
	}

	// IPC := IPC{}
	// fileIPC, err := ioutil.ReadFile(ipcFile)

	// if err != nil {
	// 	l.Fatal().
	// 		Str("Runner", runnerName).
	// 		Str("Error", err.Error()).
	// 		Str("Arquivo", ipcFile).
	// 		Msg("Erro ao ler o arquivo")
	// }

	// err = json.Unmarshal([]byte(fileIPC), &IPC)
	// if err != nil {
	// 	l.Fatal().
	// 		Str("Runner", runnerName).
	// 		Str("Error", err.Error()).
	// 		Str("Arquivo", ipcFile).
	// 		Msg("converter para struct")
	// }

	INCCDI := INCC{}
	fileINCC, err := ioutil.ReadFile(inccFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inccFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileINCC), &INCCDI)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inccFile).
			Msg("converter para struct")
	}

	INCCM := INCC{}
	fileINCCM, err := ioutil.ReadFile(inccMFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inccMFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileINCCM), &INCCM)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", inccMFile).
			Msg("converter para struct")
	}

	salarioMinimo := SalarioMinimo{}
	fileSalarioMinimo, err := ioutil.ReadFile(salarioMinimoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", salarioMinimoFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileSalarioMinimo), &salarioMinimo)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", salarioMinimoFile).
			Msg("converter para struct")
	}

	selicMeta := selic.SELICMeta{}
	fileSelic, err := ioutil.ReadFile(selicFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", selicFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileSelic), &selicMeta)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", selicFile).
			Msg("converter para struct")
	}

	selicAno := selic.SELIC{}
	fileSelicAno, err := ioutil.ReadFile(selicAnoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", selicAnoFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileSelicAno), &selicAno)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", selicAnoFile).
			Msg("converter para struct")
	}

	dividaPublica := DividaPublica{}
	fileDividaPublica, err := ioutil.ReadFile(dividaPublicaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", dividaPublicaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileDividaPublica), &dividaPublica)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", dividaPublicaFile).
			Msg("converter para struct")
	}

	pibValor := PIBValor{}
	filePIBValor, err := ioutil.ReadFile(pibValorFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pibValorFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(filePIBValor), &filePIBValor)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", dividaPublicaFile).
			Msg("converter para struct")
	}

	// Construção do map de referencias
	l.Info().
		Str("Runner", runnerName).
		Msg("Construção do map de referencias")

	for _, ip := range IPCA.Data {

		anoMesStr := fmt.Sprintf("%s%s", ip.Ano, ip.Mes)
		anoMes, _ := strconv.ParseInt(strings.TrimSpace(anoMesStr), 10, 64)

		item := Data{}
		item.Referencia = ip.Referencia
		item.Ano = ip.Ano
		item.Mes = ip.Mes
		item.AnoMes = anoMes

		if ip.Mes == "12" {
			item.ConsolidacaoAno = true
		} else {
			item.ConsolidacaoAno = false
		}

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items a Meta Selic ao Consolidado")

	for _, sl := range selicMeta.Data {

		item := consolidado[sl.Periodo]
		item.SelicMeta = sl.Valor

		consolidado[sl.Periodo] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items ao Acumulo Selic ao Consolidado")

	for _, sl := range selicAno.Data {
		item := consolidado[sl.MesAno]
		item.SelicAno = sl.Valor
		consolidado[sl.MesAno] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA ao Consolidado")

	for _, ip := range IPCA.Data {

		item := consolidado[ip.Referencia]
		item.IPCAVariacao = ip.Variacao
		item.IPCAAcumuladoAno = ip.AcumuladoAno
		item.IPCAAcumulado12Meses = ip.Acumulado12Meses

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA Detalhado ao Consolidado")

	for _, ip := range IPCADetalhadoD.Data {

		item := consolidado[ip.Referencia]
		item.IPCAVariacaoAlimentacao = ip.VariacaoAlimentacao
		item.IPCAVariacaoComunicacao = ip.VariacaoComunicacao
		item.IPCAVariacaoEducacao = ip.VariacaoEducacao
		item.IPCAVariacaoHabitacao = ip.VariacaoHabitacao
		item.IPCAVariacaoPessoais = ip.VariacaoPessoais
		item.IPCAVariacaoResidencia = ip.VariacaoArtigosResidencia
		item.IPCAVariacaoSaude = ip.VariacaoSaude
		item.IPCAVariacaoTransporte = ip.VariacaoTransporte
		item.IPCAVariacaoVestuario = ip.VariacaoVestuario

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA Detalhado Anual ao Consolidado")

	for _, ip := range IPCADetalhadoAno.Data {

		item := consolidado[ip.Referencia]
		item.IPCAAcumuladoAnoAlimentacao = ip.VariacaoAlimentacao
		item.IPCAAcumuladoAnoComunicacao = ip.VariacaoComunicacao
		item.IPCAAcumuladoAnoEducacao = ip.VariacaoEducacao
		item.IPCAAcumuladoAnoHabitacao = ip.VariacaoHabitacao
		item.IPCAAcumuladoAnoPessoais = ip.VariacaoPessoais
		item.IPCAAcumuladoAnoResidencia = ip.VariacaoArtigosResidencia
		item.IPCAAcumuladoAnoSaude = ip.VariacaoSaude
		item.IPCAAcumuladoAnoTransporte = ip.VariacaoTransporte
		item.IPCAAcumuladoAnoVestuario = ip.VariacaoVestuario

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA Detalhado 12 Meses ao Consolidado")

	for _, ip := range IPCADetalhado12Meses.Data {

		item := consolidado[ip.Referencia]
		item.IPCAAcumulado12MesesAlimentacao = ip.VariacaoAlimentacao
		item.IPCAAcumulado12MesesComunicacao = ip.VariacaoComunicacao
		item.IPCAAcumulado12MesesEducacao = ip.VariacaoEducacao
		item.IPCAAcumulado12MesesHabitacao = ip.VariacaoHabitacao
		item.IPCAAcumulado12MesesPessoais = ip.VariacaoPessoais
		item.IPCAAcumulado12MesesResidencia = ip.VariacaoArtigosResidencia
		item.IPCAAcumulado12MesesSaude = ip.VariacaoSaude
		item.IPCAAcumulado12MesesTransporte = ip.VariacaoTransporte
		item.IPCAAcumulado12MesesVestuario = ip.VariacaoVestuario

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPCA15 ao Consolidado")

	for _, ip := range IPCA15.Data {

		item := consolidado[ip.Referencia]
		item.IPCA15Variacao = ip.Variacao
		item.IPCA15AcumuladoAno = ip.AcumuladoAno
		item.IPCA15Acumulado12Meses = ip.Acumulado12Meses

		consolidado[ip.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de INPC ao Consolidado")

	for _, in := range INPC.Data {

		item := consolidado[in.Referencia]
		item.INPCVariacao = in.Variacao
		item.INPCAcumuladoAno = in.AcumuladoAno
		item.INPCAcumulado12Meses = in.Acumulado12Meses

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPA ao Consolidado")

	for _, in := range IPA.Data {

		item := consolidado[in.Referencia]
		item.IPAVariacao = in.Variacao
		item.IPAAcumuladoAno = in.AcumuladoAno

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de IPC-FIPE ao Consolidado")

	// for _, in := range IPC.Data {

	// 	item := consolidado[in.Referencia]
	// 	item.IPCVariacao = in.Variacao
	// 	item.IPCAcumuladoAno = in.AcumuladoAno

	// 	consolidado[in.Referencia] = item
	// }

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de INCC-DI ao Consolidado")

	for _, in := range INCCDI.Data {

		item := consolidado[in.Referencia]
		item.INCCVariacao = in.Variacao
		item.INCCAcumuladoAno = in.AcumuladoAno

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Agregando os Items de INCC-M ao Consolidado")

	for _, in := range INCCM.Data {

		item := consolidado[in.Referencia]
		item.INCCMVariacao = in.Variacao
		item.INCCMcumuladoAno = in.AcumuladoAno

		consolidado[in.Referencia] = item
	}

	for _, in := range salarioMinimo.Data {

		item := consolidado[in.Referencia]
		item.SalarioMinimo = in.Valor

		consolidado[in.Referencia] = item
	}

	for _, in := range dividaPublica.Data {

		item := consolidado[in.Referencia]
		item.DividaPublica = in.Valor

		consolidado[in.Referencia] = item
	}

	for _, in := range pibValor.Data {

		item := consolidado[in.Referencia]
		item.PibValor = in.Valor

		consolidado[in.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Criando o Struct Final de Inflação")

	for _, i := range consolidado {
		if i.Referencia != "" {
			inflacao.Data = append(inflacao.Data, i)
		}
	}

	// Sort do data
	sort.Slice(inflacao.Data, func(i, j int) bool {
		return inflacao.Data[i].Referencia < inflacao.Data[j].Referencia
	})

	// Preenchendo os intervalos de 45 dias da meta selic
	for k, _ := range inflacao.Data {
		item := inflacao.Data[k]
		if item.SelicMeta == 0 && k != 0 {
			it := inflacao.Data[k-1]
			item.SelicMeta = it.SelicMeta
			inflacao.Data[k] = item
		}
	}

	// Calculando o Juros Reais // IPCA vs SELIC - TROCAR PARA O SELIC ANUAL
	for k, i := range inflacao.Data {
		if i.IPCAAcumulado12Meses != 0 && i.SelicAno != 0 {
			juros_reais := (((1 + i.SelicAno/100) / (1 + i.IPCAAcumulado12Meses/100)) - 1) * 100
			juros_str := strconv.FormatFloat(juros_reais, 'f', 2, 64)
			juros_reais, err := strconv.ParseFloat(strings.TrimSpace(juros_str), 64)
			if err != nil {
				l.Fatal().
					Str("Runner", runnerName).
					Str("Error", err.Error()).
					Str("Valor recuperado", juros_str).
					Msg("Erro ao converter o valor para Float64")
			}

			i.JurosReais = juros_reais
		}
		inflacao.Data[k] = i
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(inflacao)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter a struct em JSON")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Criando arquivo de persistência para os dados convertidos")

	f, err := os.Create(file_path)
	defer f.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro ao criar o diretório para persistência dos dados")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Iniciando a escrita dos dados no arquivo de persistência")

	_, err = f.WriteString(string(b))

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	// Convertendo em CSV
	csvFile, err := os.OpenFile(fileNameOutputCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao criar o dataset em CSV")
	}
	defer csvFile.Close()

	csvOutput, err := gocsv.MarshalString(&inflacao.Data)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao escrever o dataset em CSV")
	}

	_, err = csvFile.WriteString(string(csvOutput))
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Dataset em CSV Criado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Str("S3Key", s3KeyCSV).
		Msg("Fazendo Upload para o S3")

	err = upload.S3(fileNameOutputCSV, s3KeyCSV)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("S3Key", s3KeyCSV).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	err = upload.S3(file_path, s3KeyJSON)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("S3Key", s3KeyJSON).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")

}
