package inflacao

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/gocarina/gocsv"
)

type IPCADetalhadoResponse []struct {
	ID         string `json:"id"`
	Variavel   string `json:"variavel"`
	Unidade    string `json:"unidade"`
	Resultados []struct {
		Classificacoes []struct {
			ID        string `json:"id"`
			Nome      string `json:"nome"`
			Categoria struct {
				Num7169 string `json:"7169"`
			} `json:"categoria"`
		} `json:"classificacoes"`
		Series []struct {
			Localidade struct {
				ID    string `json:"id"`
				Nivel struct {
					ID   string `json:"id"`
					Nome string `json:"nome"`
				} `json:"nivel"`
				Nome string `json:"nome"`
			} `json:"localidade"`
			Serie struct {
				Num201201 string `json:"201201"`
				Num201202 string `json:"201202"`
				Num201203 string `json:"201203"`
				Num201204 string `json:"201204"`
				Num201205 string `json:"201205"`
				Num201206 string `json:"201206"`
				Num201207 string `json:"201207"`
				Num201208 string `json:"201208"`
				Num201209 string `json:"201209"`
				Num201210 string `json:"201210"`
				Num201211 string `json:"201211"`
				Num201212 string `json:"201212"`
				Num201301 string `json:"201301"`
				Num201302 string `json:"201302"`
				Num201303 string `json:"201303"`
				Num201304 string `json:"201304"`
				Num201305 string `json:"201305"`
				Num201306 string `json:"201306"`
				Num201307 string `json:"201307"`
				Num201308 string `json:"201308"`
				Num201309 string `json:"201309"`
				Num201310 string `json:"201310"`
				Num201311 string `json:"201311"`
				Num201312 string `json:"201312"`
				Num201401 string `json:"201401"`
				Num201402 string `json:"201402"`
				Num201403 string `json:"201403"`
				Num201404 string `json:"201404"`
				Num201405 string `json:"201405"`
				Num201406 string `json:"201406"`
				Num201407 string `json:"201407"`
				Num201408 string `json:"201408"`
				Num201409 string `json:"201409"`
				Num201410 string `json:"201410"`
				Num201411 string `json:"201411"`
				Num201412 string `json:"201412"`
				Num201501 string `json:"201501"`
				Num201502 string `json:"201502"`
				Num201503 string `json:"201503"`
				Num201504 string `json:"201504"`
				Num201505 string `json:"201505"`
				Num201506 string `json:"201506"`
				Num201507 string `json:"201507"`
				Num201508 string `json:"201508"`
				Num201509 string `json:"201509"`
				Num201510 string `json:"201510"`
				Num201511 string `json:"201511"`
				Num201512 string `json:"201512"`
				Num201601 string `json:"201601"`
				Num201602 string `json:"201602"`
				Num201603 string `json:"201603"`
				Num201604 string `json:"201604"`
				Num201605 string `json:"201605"`
				Num201606 string `json:"201606"`
				Num201607 string `json:"201607"`
				Num201608 string `json:"201608"`
				Num201609 string `json:"201609"`
				Num201610 string `json:"201610"`
				Num201611 string `json:"201611"`
				Num201612 string `json:"201612"`
				Num201701 string `json:"201701"`
				Num201702 string `json:"201702"`
				Num201703 string `json:"201703"`
				Num201704 string `json:"201704"`
				Num201705 string `json:"201705"`
				Num201706 string `json:"201706"`
				Num201707 string `json:"201707"`
				Num201708 string `json:"201708"`
				Num201709 string `json:"201709"`
				Num201710 string `json:"201710"`
				Num201711 string `json:"201711"`
				Num201712 string `json:"201712"`
				Num201801 string `json:"201801"`
				Num201802 string `json:"201802"`
				Num201803 string `json:"201803"`
				Num201804 string `json:"201804"`
				Num201805 string `json:"201805"`
				Num201806 string `json:"201806"`
				Num201807 string `json:"201807"`
				Num201808 string `json:"201808"`
				Num201809 string `json:"201809"`
				Num201810 string `json:"201810"`
				Num201811 string `json:"201811"`
				Num201812 string `json:"201812"`
				Num201901 string `json:"201901"`
				Num201902 string `json:"201902"`
				Num201903 string `json:"201903"`
				Num201904 string `json:"201904"`
				Num201905 string `json:"201905"`
				Num201906 string `json:"201906"`
				Num201907 string `json:"201907"`
				Num201908 string `json:"201908"`
				Num201909 string `json:"201909"`
				Num201910 string `json:"201910"`
				Num201911 string `json:"201911"`
				Num201912 string `json:"201912"`
				Num202001 string `json:"202001"`
				Num202002 string `json:"202002"`
				Num202003 string `json:"202003"`
				Num202004 string `json:"202004"`
				Num202005 string `json:"202005"`
				Num202006 string `json:"202006"`
				Num202007 string `json:"202007"`
				Num202008 string `json:"202008"`
				Num202009 string `json:"202009"`
				Num202010 string `json:"202010"`
				Num202011 string `json:"202011"`
				Num202012 string `json:"202012"`
				Num202101 string `json:"202101"`
				Num202102 string `json:"202102"`
				Num202103 string `json:"202103"`
				Num202104 string `json:"202104"`
				Num202105 string `json:"202105"`
				Num202106 string `json:"202106"`
				Num202107 string `json:"202107"`
				Num202108 string `json:"202108"`
				Num202109 string `json:"202109"`
				Num202110 string `json:"202110"`
				Num202111 string `json:"202111"`
				Num202112 string `json:"202112"`
				Num202201 string `json:"202201"`
				Num202202 string `json:"202202"`
				Num202203 string `json:"202203"`
				Num202204 string `json:"202204"`
				Num202205 string `json:"202205"`
				Num202206 string `json:"202206"`
				Num202207 string `json:"202207"`
				Num202208 string `json:"202208"`
				Num202209 string `json:"202209"`
				Num202210 string `json:"202210"`
				Num202211 string `json:"202211"`
				Num202212 string `json:"202212"`
				Num202301 string `json:"202301"`
				Num202302 string `json:"202302"`
				Num202303 string `json:"202303"`
				Num202304 string `json:"202304"`
				Num202305 string `json:"202305"`
				Num202306 string `json:"202306"`
				Num202307 string `json:"202307"`
				Num202308 string `json:"202308"`
				Num202309 string `json:"202309"`
				Num202310 string `json:"202310"`
				Num202311 string `json:"202311"`
				Num202312 string `json:"202312"`
				Num202401 string `json:"202401"`
				Num202402 string `json:"202402"`
				Num202403 string `json:"202403"`
				Num202404 string `json:"202404"`
				Num202405 string `json:"202405"`
				Num202406 string `json:"202406"`
				Num202407 string `json:"202407"`
				Num202408 string `json:"202408"`
				Num202409 string `json:"202409"`
				Num202410 string `json:"202410"`
				Num202411 string `json:"202411"`
				Num202412 string `json:"202412"`
				Num202501 string `json:"202501"`
				Num202502 string `json:"202502"`
				Num202503 string `json:"202503"`
				Num202504 string `json:"202504"`
				Num202505 string `json:"202505"`
				Num202506 string `json:"202506"`
				Num202507 string `json:"202507"`
				Num202508 string `json:"202508"`
				Num202509 string `json:"202509"`
				Num202510 string `json:"202510"`
				Num202511 string `json:"202511"`
				Num202512 string `json:"202512"`
				Num202601 string `json:"202601"`
				Num202602 string `json:"202602"`
				Num202603 string `json:"202603"`
				Num202604 string `json:"202604"`
				Num202605 string `json:"202605"`
				Num202606 string `json:"202606"`
				Num202607 string `json:"202607"`
				Num202608 string `json:"202608"`
				Num202609 string `json:"202609"`
				Num202610 string `json:"202610"`
				Num202611 string `json:"202611"`
				Num202612 string `json:"202612"`
			} `json:"serie"`
		} `json:"series"`
	} `json:"resultados"`
}

type DataIPCADetalhado struct {
	Referencia                string  `json:"referencia" csv:"referencia"`
	Ano                       string  `json:"ano" csv:"ano"`
	Mes                       string  `json:"mes" csv:"mes"`
	Variacao                  float64 `json:"variacao_geral" csv:"variacao_geral"`
	VariacaoAlimentacao       float64 `json:"variacao_alimentacao" csv:"variacao_alimentacao"`
	VariacaoHabitacao         float64 `json:"variacao_habitacao" csv:"variacao_habitacao"`
	VariacaoArtigosResidencia float64 `json:"variacao_artigos_residencia" csv:"variacao_artigos_residencia"`
	VariacaoVestuario         float64 `json:"variacao_vestuario" csv:"variacao_vestuario"`
	VariacaoTransporte        float64 `json:"variacao_transporte" csv:"variacao_transporte"`
	VariacaoSaude             float64 `json:"variacao_saude_cuidados_pessoais" csv:"variacao_saude_cuidados_pessoais"`
	VariacaoPessoais          float64 `json:"variacao_despesas_pessoais" csv:"variacao_despesas_pessoais"`
	VariacaoEducacao          float64 `json:"variacao_educacao" csv:"variacao_educacao"`
	VariacaoComunicacao       float64 `json:"variacao_comunicacao" csv:"variacao_comunicacao"`

	VariacaoAlimentacaoDec       float64 `json:"variacao_alimentacao_dec" csv:"variacao_alimentacao_dec"`
	VariacaoHabitacaoDec         float64 `json:"variacao_habitacao_dec" csv:"variacao_habitacao_dec"`
	VariacaoArtigosResidenciaDec float64 `json:"variacao_artigos_residencia_dec" csv:"variacao_artigos_residencia_dec"`
	VariacaoVestuarioDec         float64 `json:"variacao_vestuario_dec" csv:"variacao_vestuario_dec"`
	VariacaoTransporteDec        float64 `json:"variacao_transporte_dec" csv:"variacao_transporte_dec"`
	VariacaoSaudeDec             float64 `json:"variacao_saude_cuidados_pessoais_dec" csv:"variacao_saude_cuidados_pessoais_dec"`
	VariacaoPessoaisDec          float64 `json:"variacao_despesas_pessoais_dec" csv:"variacao_despesas_pessoais_dec"`
	VariacaoEducacaoDec          float64 `json:"variacao_educacao_dec" csv:"variacao_educacao_dec"`
	VariacaoComunicacaoDec       float64 `json:"variacao_comunicacao_dec" csv:"variacao_comunicacao_dec"`

	ConsolidacaoAno   bool   `json:"consolidado_ano" csv:"consolidado_ano"`
	IdentificadorIBGE string `json:"identificador_ibge" csv:"identificador_ibge"`
}

type IPCADetalhado struct {
	Atualizacao   time.Time           `json:"data_atualizacao"`
	UnidadeMedida string              `json:"unidade_medida"`
	Fonte         string              `json:"fonte"`
	Data          []DataIPCADetalhado `json:"data"`
}

func RunnerIPCADetalhado() {
	runnerName := "IPCA - Detalhado"
	url_old := "https://servicodados.ibge.gov.br/api/v3/agregados/1419/periodos/-999999/variaveis/63?localidades=N1%5Ball%5D&classificacao=315%5B7169,7170,7445,7486,7558,7625,7660,7712,7766,7786%5D"
	url := "https://servicodados.ibge.gov.br/api/v3/agregados/7060/periodos/-99999/variaveis/63?localidades=N1%5Ball%5D&classificacao=315%5B7169,7170,7445,7486,7558,7625,7660,7712,7766,7786%5D"
	unidadeMedida := "%"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/inflacao/ipca_detalhado.json"
	fileNameOutputCSV := "./data/inflacao/ipca_detalhado.csv"

	s3KeyCSV := "inflacao/ipca_detalhado.csv"
	s3KeyJSON := "inflacao/ipca_detalhado.json"

	ordenado := make(map[string]DataIPCADetalhado)

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	ipca := IPCADetalhado{}
	now := time.Now()
	ipca.Atualizacao = now
	ipca.Fonte = fonte
	ipca.UnidadeMedida = unidadeMedida

	var response_old IPCADetalhadoResponse
	var response IPCADetalhadoResponse

	resOld, err := http.Get(url_old)
	defer resOld.Body.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTP para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	decoder_old := json.NewDecoder(resOld.Body)
	err = decoder_old.Decode(&response_old)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	res, err := http.Get(url)
	defer res.Body.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTP para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	names := structs.Names(response[0].Resultados[0].Series[0].Serie)

	// Criando o Map de Referencias
	for _, n := range names {
		item := DataIPCADetalhado{}

		anomes := strings.Replace(n, "Num", "", -1)
		ano := anomes[0:4]
		mes := anomes[4:6]

		referencia := fmt.Sprintf("%v-%v", ano, mes)
		item.Referencia = referencia
		item.Mes = mes
		item.Ano = ano
		item.IdentificadorIBGE = n

		if mes == "12" {
			item.ConsolidacaoAno = true
		} else {
			item.ConsolidacaoAno = false
		}

		ordenado[n] = item
	}
	// Indice Geral
	geraisOld := response_old[0].Resultados[0].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(geraisOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.Variacao = valor

		ordenado[n] = item
	}

	gerais := response[0].Resultados[0].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(gerais)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.Variacao = valor

		ordenado[n] = item
	}

	// Alimentacao
	alimentacaoOld := response_old[0].Resultados[1].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(alimentacaoOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoAlimentacao = valor
		item.VariacaoAlimentacaoDec = valor * 0.193
		ordenado[n] = item
	}

	alimentacao := response[0].Resultados[1].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(alimentacao)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoAlimentacao = valor
		item.VariacaoAlimentacaoDec = valor * 0.193

		ordenado[n] = item
	}

	habitacaoOld := response_old[0].Resultados[2].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(habitacaoOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoHabitacao = valor
		item.VariacaoHabitacaoDec = valor * 0.156

		ordenado[n] = item
	}

	habitacao := response[0].Resultados[2].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(habitacao)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoHabitacao = valor
		item.VariacaoHabitacaoDec = valor * 0.156

		ordenado[n] = item
	}

	// Variação Artigos Residência
	residenciaOld := response_old[0].Resultados[3].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(residenciaOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoArtigosResidencia = valor
		item.VariacaoArtigosResidenciaDec = valor * 0.038
		ordenado[n] = item
	}

	residencia := response[0].Resultados[3].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(residencia)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoArtigosResidencia = valor
		item.VariacaoArtigosResidenciaDec = valor * 0.038

		ordenado[n] = item
	}

	vestuarioOld := response_old[0].Resultados[4].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(vestuarioOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoVestuario = valor
		item.VariacaoVestuarioDec = valor * 0.046

		ordenado[n] = item
	}

	vestuario := response[0].Resultados[4].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(vestuario)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoVestuario = valor
		item.VariacaoVestuarioDec = valor * 0.046

		ordenado[n] = item
	}

	transporteOld := response_old[0].Resultados[5].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(transporteOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoTransporte = valor
		item.VariacaoTransporteDec = valor * 0.206

		ordenado[n] = item
	}

	transporte := response[0].Resultados[5].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(transporte)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoTransporte = valor
		item.VariacaoTransporteDec = valor * 0.206

		ordenado[n] = item
	}

	saudeOld := response_old[0].Resultados[6].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(saudeOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoSaude = valor
		item.VariacaoSaudeDec = valor * 0.135

		ordenado[n] = item
	}

	saude := response[0].Resultados[6].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(saude)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoSaude = valor
		item.VariacaoSaudeDec = valor * 0.135

		ordenado[n] = item
	}

	pessoaisOld := response_old[0].Resultados[7].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(pessoaisOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoPessoais = valor
		item.VariacaoPessoaisDec = valor * 0.107

		ordenado[n] = item
	}

	pessoais := response[0].Resultados[7].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(pessoais)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoPessoais = valor
		item.VariacaoPessoaisDec = valor * 0.107

		ordenado[n] = item
	}

	educacaoOld := response_old[0].Resultados[8].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(educacaoOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoEducacao = valor
		item.VariacaoEducacaoDec = valor * 0.061

		ordenado[n] = item
	}

	educacao := response[0].Resultados[8].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(educacao)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoEducacao = valor
		item.VariacaoEducacaoDec = valor * 0.061

		ordenado[n] = item
	}

	comunicacaoOld := response_old[0].Resultados[9].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(comunicacaoOld)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoComunicacao = valor
		item.VariacaoComunicacaoDec = valor * 0.057

		ordenado[n] = item
	}

	comunicacao := response[0].Resultados[9].Series[0].Serie
	for _, n := range names {
		r := reflect.ValueOf(comunicacao)
		f := reflect.Indirect(r).FieldByName(n)

		valueRaw := f.String()

		if valueRaw == "..." || valueRaw == "" {
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Error().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
			continue
		}

		item := ordenado[n]
		item.VariacaoComunicacao = valor
		item.VariacaoComunicacaoDec = valor * 0.057

		ordenado[n] = item
	}

	for _, i := range ordenado {
		if i.Variacao == 0 {
			continue
		}
		ipca.Data = append(ipca.Data, i)
	}

	// Sort do data
	sort.Slice(ipca.Data, func(i, j int) bool {
		return ipca.Data[i].Referencia < ipca.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(ipca)
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

	csvOutput, err := gocsv.MarshalString(&ipca.Data)
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
