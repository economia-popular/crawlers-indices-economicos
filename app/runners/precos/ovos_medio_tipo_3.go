package precos

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/gocolly/colly"
	"github.com/rs/zerolog/log"
)

type DataOvoMedioTipo330duzias struct {
	Referencia string  `json:"referencia" csv:"referencia"`
	Valor      float64 `json:"valor" csv:"valor"`
	ModoVenda  string  `json:"modo_venda" csv:"modo_venda"`
	Duzias     float64 `json:"duzias" csv:"duzias"`
}

type OvoMedioTipo330Duzias struct {
	Atualizacao time.Time                   `json:"data_atualizacao"`
	Fonte       string                      `json:"fonte"`
	Data        []DataOvoMedioTipo330duzias `json:"data"`
}

func RunnerOvoMedioTipo3() {
	runnerName := "Preços - OvoMedioTipo3 30 Duzias"
	domain := "www.ipeadata.gov.br"
	url := "http://www.ipeadata.gov.br/ExibeSerie.aspx?serid=37633&module=M"
	file_path := "./data/precos/ovomediotipo3-30duzias.json"
	fileNameOutputCSV := "./data/precos/ovomediotipo3-30duzias.csv"
	s3KeyJSON := "precos/ovomediotipo3-30duzias.json"
	s3KeyCSV := "precos/ovomediotipo3-30duzias.csv"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector(
		colly.AllowedDomains(domain),
	)

	ovomediotipo3 := &OvoMedioTipo330Duzias{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	ovomediotipo3.Atualizacao = now
	ovomediotipo3.Fonte = url

	c.OnHTML(".dxgvTable", func(e *colly.HTMLElement) {

		l.Info().
			Str("Runner", runnerName).
			Str("Domain", domain).
			Str("URL", url).
			Msg("Recuperando o HTML da Página")

		l.Info().
			Str("Runner", runnerName).
			Str("Domain", domain).
			Str("URL", url).
			Msg("Encontrando o elemento <table> para efetuar o parsing")

		e.ForEach("tr", func(i int, tr *colly.HTMLElement) {

			mes_referencia_td := strings.Replace(tr.ChildText("td:nth-child(1)"), ",", ".", -1)
			valor_td := strings.Replace(tr.ChildText("td:nth-child(2)"), ",", ".", -1)

			valor, err := strconv.ParseFloat(strings.TrimSpace(valor_td), 64)

			if mes_referencia_td == "" || valor_td == "" {
				return
			}

			if err != nil {
				l.Error().
					Str("Runner", runnerName).
					Str("Error", err.Error()).
					Str("Valor recuperado", valor_td).
					Msg("Erro ao converter o valor para Float64")

				return
			}

			referencia := strings.Replace(mes_referencia_td, ".", "-", -1)

			log.Info().
				Str("Runner", runnerName).
				Str("Domain", domain).
				Str("Referencia", referencia).
				Float64("Valor", valor).
				Msg("Item normalizado")

			item := DataOvoMedioTipo330duzias{
				Referencia: referencia,
				Valor:      valor,
				ModoVenda:  "atacado",
				Duzias:     30.00,
			}

			ovomediotipo3.Data = append(ovomediotipo3.Data, item)

		})

		l.Info().
			Str("Runner", runnerName).
			Msg("Convertendo a Struct do Schema em formato JSON")

		b, err := json.Marshal(ovomediotipo3)
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

		csvOutput, err := gocsv.MarshalString(&ovomediotipo3.Data)
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
