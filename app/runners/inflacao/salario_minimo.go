package inflacao

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/gocolly/colly"
)

type DataSalarioMinimo struct {
	Referencia  string  `json:"referencia" csv:"referencia"`
	Ano         string  `json:"ano" csv:"ano"`
	Mes         string  `json:"mes" csv:"mes"`
	Anomes      string  `json:"ano_mes csv:"ano_mes"`
	Valor       float64 `json:"valor" csv:"valor"`
	Consolidado bool    `json:"consolidado" csv:"consolidado"`
}

type SalarioMinimo struct {
	Atualizacao time.Time           `json:"data_atualizacao"`
	Fonte       string              `json:"fonte"`
	Data        []DataSalarioMinimo `json:"data"`
}

func RunnerSalarioMinimo() {
	runnerName := "Salário Minimo"
	domain := "www.ipeadata.gov.br"
	url := "http://www.ipeadata.gov.br/ExibeSerie.aspx?serid=1739471028"
	file_path := "./data/inflacao/salario_minimo.json"
	fileNameOutputCSV := "./data/inflacao/salario_minimo.csv"

	s3KeyCSV := "inflacao/salario_minimo.csv"
	s3KeyJSON := "inflacao/salario_minimo.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector(
		colly.AllowedDomains(domain),
	)

	indice := &SalarioMinimo{}
	indice.Atualizacao = time.Now()
	indice.Fonte = domain

	c.OnHTML(".dxgvTable", func(e *colly.HTMLElement) {

		fmt.Println("Entrou")

		e.ForEach("tr", func(i int, tr *colly.HTMLElement) {

			mes_referencia_td := strings.Replace(tr.ChildText("td:nth-child(1)"), ",", ".", -1)
			valor_td := strings.Replace(strings.Replace(tr.ChildText("td:nth-child(2)"), ".", "", -1), ",", ".", -1)

			valor, err := strconv.ParseFloat(strings.TrimSpace(valor_td), 64)

			if mes_referencia_td == "" || valor_td == "" {
				return
			}
			referencia := strings.Replace(mes_referencia_td, ".", "-", -1)
			ano := referencia[0:4]
			mes := referencia[5:7]

			if err != nil {
				l.Error().
					Str("Runner", runnerName).
					Str("Error", err.Error()).
					Str("Valor recuperado", valor_td).
					Msg("Erro ao converter o valor para Float64")

				return
			}

			item := DataSalarioMinimo{
				Referencia: referencia,
				Valor:      valor,
				Ano:        ano,
				Mes:        mes,
			}

			indice.Data = append(indice.Data, item)

		})

		l.Info().
			Str("Runner", runnerName).
			Msg("Convertendo a Struct do Schema em formato JSON")

		b, err := json.Marshal(indice)
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

		csvOutput, err := gocsv.MarshalString(&indice.Data)
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
	})

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Efetuando requisição para o Endpoint")

	c.Visit(url)
}
