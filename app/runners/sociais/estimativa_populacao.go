package sociais

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/gocolly/colly"
)

type DataPopulacional struct {
	Referencia string `json:"referencia" csv:"referencia"`
	Total      int64  `json:"total" csv:"total"`
}

type EstimativaPopulacional struct {
	Atualizacao   time.Time          `json:"data_atualizacao"`
	Fonte         string             `json:"fonte"`
	UnidadeMedida string             `json:"unidade_medida"`
	Data          []DataPopulacional `json:"data"`
}

func RunnerEstimativaPopulacao() {
	runnerName := "Estimativa da População"
	domain := "aplicacoes.cidadania.gov.br"
	url := "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q[]=r6JtY5KygrBtxKW25rV%2FfmdhhJJll21kmK19Zm11ZmymaX7KrV6OmWqlo5TJ7rJvsLqqn7R0s6%2BjoLycbt2yoNnAwKiltZau4q%2BufXqcxdWU0aKgfd%2BuVIy3pa%2FlmhD1%2BtDG3aOl"
	unidadeMedida := "Numero de Pessoas"
	fonte := "https://aplicacoes.cidadania.gov.br | https://www.ibge.gov.br"
	file_path := "./data/sociais/estimativa_populacional.json"
	fileNameOutputCSV := "./data/sociais/estimativa_populacional.csv"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector()

	indice := &EstimativaPopulacional{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	indice.Atualizacao = now
	indice.Fonte = fonte
	indice.UnidadeMedida = unidadeMedida

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

			referencia_td := strings.Replace(tr.ChildText("td:nth-child(1)"), ".", "", -1)
			valor_td := strings.Replace(tr.ChildText("td:nth-child(2)"), ".", "", -1)
			valor, _ := strconv.ParseInt(strings.TrimSpace(valor_td), 10, 64)

			if valor > 0 && strings.HasPrefix(referencia_td, "12") {

				item := DataPopulacional{
					Referencia: referencia_td[3:7],
					Total:      valor,
				}

				l.Info().
					Str("Runner", runnerName).
					Str("Domain", domain).
					Str("Referencia", referencia_td).
					Int64("Numero de Pessoas em Pobreza/Extrema Pobreza", valor).
					Msg("Adicionando item ao dataset")

				indice.Data = append(indice.Data, item)
			}

		})

		// Fix até sair o Dataset de 2022 no Cadastro Unico

		item := DataPopulacional{
			Referencia: "2022",
			Total:      213317639,
		}

		indice.Data = append(indice.Data, item)

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
