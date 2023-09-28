package gini

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
)

type Data struct {
	Ano   string  `json:"ano_referencia" csv:"ano_referencia"`
	Valor float64 `json:"valor" csv:"valor"`
}

type GINI struct {
	Atualizacao time.Time `json:"data_atualizacao"`
	Fonte       string    `json:"fonte"`
	Data        []Data    `json:"data"`
}

func Runner() {
	runnerName := "GINI"
	domain := "www.indexmundi.com"
	url := "https://www.indexmundi.com/facts/brazil/indicator/SI.POV.GINI"
	file_path := "./data/gini/gini.json"
	fileNameOutputCSV := "./data/gini/gini.csv"

	s3KeyCSV := "gini/gini.csv"
	s3KeyJSON := "gini/gini.json"

	fonte := "indexmundi.com / data.worldbank.org"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector()

	gini := &GINI{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	gini.Atualizacao = now
	gini.Fonte = fonte

	// Find and print all links
	c.OnHTML("table", func(e *colly.HTMLElement) {

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

			l.Info().
				Str("Runner", runnerName).
				Str("URL", url).
				Msg("Item recuparado itens do elemento <table>")

			l.Info().
				Str("Runner", runnerName).
				Str("URL", url).
				Msg("Removendo a primeira linha da tabela Year/Value")

			if i == 0 {
				return
			}

			l.Info().
				Str("Runner", runnerName).
				Str("URL", url).
				Msg("Normalizando os dados recuperados")

			ano := strings.Replace(tr.ChildText("td:nth-child(1)"), ",", ".", -1)
			index := strings.Replace(tr.ChildText("td:nth-child(2)"), ",", ".", -1)

			valor, _ := strconv.ParseFloat(strings.TrimSpace(index), 64)

			item := Data{
				Ano:   ano,
				Valor: valor / 100,
			}

			gini.Data = append(gini.Data, item)

		})

		// Adicionando os anos manualmente por encontrá-los somente em noticias
		// https://www.ipea.gov.br/cartadeconjuntura/index.php/tag/desigualdade-de-renda/

		item2021 := Data{
			Ano:   "2021",
			Valor: 0.489,
		}

		gini.Data = append(gini.Data, item2021)

		item2022 := Data{
			Ano:   "2022",
			Valor: 0.489,
		}

		gini.Data = append(gini.Data, item2022)

		l.Info().
			Str("Runner", runnerName).
			Msg("Convertendo a Struct do Schema em formato JSON")

		b, err := json.Marshal(gini)
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

		csvOutput, err := gocsv.MarshalString(&gini.Data)
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
	})

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Efetuando requisição para o Endpoint")

	c.Visit(url)
}
