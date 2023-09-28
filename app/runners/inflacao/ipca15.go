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

type DataIPCA15 struct {
	Referencia        string  `json:"referencia" csv:"referencia"`
	Ano               string  `json:"ano" csv:"ano"`
	Mes               string  `json:"mes" csv:"mes"`
	Variacao          float64 `json:"variacao" csv:"variacao"`
	AcumuladoAno      float64 `json:"acumulado_ano" csv:"acumulado_ano"`
	Acumulado12Meses  float64 `json:"acumulado_doze_meses" csv:"acumulado_doze_meses"`
	ConsolidacaoAno   bool    `json:"consolidado_ano" csv:"consolidado_ano"`
	IdentificadorIBGE string  `json:"identificador_ibge" csv:"identificador_ibge"`
}

type IPCA15 struct {
	Atualizacao   time.Time    `json:"data_atualizacao"`
	UnidadeMedida string       `json:"unidade_medida"`
	Fonte         string       `json:"fonte"`
	Data          []DataIPCA15 `json:"data"`
}

func RunnerIPCA15() {

	runnerName := "IPCA15 - Histórico"
	url := "https://servicodados.ibge.gov.br/api/v3/agregados/3065/periodos/-99999/variaveis/355%7C356%7C1120?localidades=N1%5Ball%5D"
	unidadeMedida := "%"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/inflacao/ipca15.json"
	fileNameOutputCSV := "./data/inflacao/ipca15.csv"

	s3KeyCSV := "inflacao/ipca15.csv"
	s3KeyJSON := "inflacao/ipca15.json"

	ordenado := make(map[string]DataIPCA15)

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	ipca := IPCA15{}
	now := time.Now()
	ipca.Atualizacao = now
	ipca.Fonte = fonte
	ipca.UnidadeMedida = unidadeMedida

	var response IPCAResponse
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
		item := DataIPCA15{}

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

	for _, r := range response {

		// Variação Mensal
		if strings.HasSuffix(r.Variavel, "Variação mensal") {
			names := structs.Names(r.Resultados[0].Series[0].Serie)

			for _, n := range names {
				r := reflect.ValueOf(r.Resultados[0].Series[0].Serie)
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
		}

		// Acumulado Ano
		if strings.HasSuffix(r.Variavel, "Variação acumulada no ano") {
			names := structs.Names(r.Resultados[0].Series[0].Serie)

			for _, n := range names {
				r := reflect.ValueOf(r.Resultados[0].Series[0].Serie)
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
				item.AcumuladoAno = valor

				ordenado[n] = item
			}
		}

		// Acumulado 12 Meses
		if strings.HasSuffix(r.Variavel, "Variação acumulada em 12 meses") {
			names := structs.Names(r.Resultados[0].Series[0].Serie)

			for _, n := range names {
				r := reflect.ValueOf(r.Resultados[0].Series[0].Serie)
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
				item.Acumulado12Meses = valor

				ordenado[n] = item
			}
		}

	}

	for _, i := range ordenado {
		if i.AcumuladoAno == 0 || i.Acumulado12Meses == 0 || i.Variacao == 0 || i.Referencia == "" {
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
		Str("FilePath", fileNameOutputCSV).
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")
}
