package inflacao

import (
	"crawlers/pkg/logger"
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

func RunnerIPCADetalhadoAno() {
	runnerName := "IPCA - Detalhado Anual"
	url_old := "https://servicodados.ibge.gov.br/api/v3/agregados/1419/periodos/-999999/variaveis/69?localidades=N1[all]&classificacao=315[7169,7170,7445,7486,7558,7625,7660,7712,7766,7786]"
	url := "https://servicodados.ibge.gov.br/api/v3/agregados/7060/periodos/-99999/variaveis/69?localidades=N1[all]&classificacao=315[7169,7170,7445,7486,7558,7625,7660,7712,7766,7786]"
	unidadeMedida := "%"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/inflacao/ipca_detalhado_ano.json"
	fileNameOutputCSV := "./data/inflacao/ipca_detalhado_ano.csv"

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
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")

}
