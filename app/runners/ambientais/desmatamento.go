package ambientais

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

type DataDesmatamento struct {
	Referencia int64 `json:"referencia" csv:"referencia"`
	Acre       int64 `json:"acre" csv:"acre"`
	Amazonas   int64 `json:"amazonas" csv:"amazonas"`
	Amapa      int64 `json:"amapa" csv:"amapa"`
	Maranhao   int64 `json:"maranhao" csv:"maranhao"`
	MatoGrosso int64 `json:"mato_grosso" csv:"mato_grosso"`
	Para       int64 `json:"para" csv:"para"`
	Rondonia   int64 `json:"rondonia" csv:"rondonia"`
	Roraima    int64 `json:"roraima" csv:"roraima"`
	Tocantins  int64 `json:"tocantins" csv:"tocantins"`
	AreaTotal  int64 `json:"area_total_desmatamento" csv:"area_total_desmatamento"`
}

type Desmatamento struct {
	Atualizacao   time.Time          `json:"data_atualizacao"`
	Fonte         string             `json:"fonte"`
	UnidadeMedida string             `json:"unidade_medida"`
	Data          []DataDesmatamento `json:"data"`
}

func RunnerDesmatamentoProdes() {
	runnerName := "Desmatamento - PRODES"
	domain := "www.obt.inpe.br"
	url := "http://www.obt.inpe.br/OBT/assuntos/programas/amazonia/prodes"
	file_path := "./data/ambientais/desmatamento_prodes.json"
	fileNameOutputCSV := "./data/ambientais/desmatamento_prodes.csv"

	s3KeyCSV := "ambientais/desmatamento_prodes.csv"
	s3KeyJSON := "ambientais/desmatamento_prodes.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector()

	indice := &Desmatamento{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	indice.Atualizacao = now
	indice.Fonte = url
	indice.UnidadeMedida = "km2"

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

			referencia_td := strings.Replace(tr.ChildText("td:nth-child(1)"), "*", "", -1)
			referencia, _ := strconv.ParseInt(strings.TrimSpace(referencia_td), 10, 64)

			acre_td := tr.ChildText("td:nth-child(2)")
			acre, _ := strconv.ParseInt(strings.TrimSpace(acre_td), 10, 64)

			amazonas_td := tr.ChildText("td:nth-child(3)")
			amazonas, _ := strconv.ParseInt(strings.TrimSpace(amazonas_td), 10, 64)

			amapa_td := tr.ChildText("td:nth-child(4)")
			amapa, _ := strconv.ParseInt(strings.TrimSpace(amapa_td), 10, 64)

			maranhao_td := tr.ChildText("td:nth-child(5)")
			maranhao, _ := strconv.ParseInt(strings.TrimSpace(maranhao_td), 10, 64)

			mato_grosso_td := tr.ChildText("td:nth-child(6)")
			mato_grosso, _ := strconv.ParseInt(strings.TrimSpace(mato_grosso_td), 10, 64)

			para_td := tr.ChildText("td:nth-child(7)")
			para, _ := strconv.ParseInt(strings.TrimSpace(para_td), 10, 64)

			rondonia_td := tr.ChildText("td:nth-child(8)")
			rondonia, _ := strconv.ParseInt(strings.TrimSpace(rondonia_td), 10, 64)

			roraima_td := tr.ChildText("td:nth-child(9)")
			roraima, _ := strconv.ParseInt(strings.TrimSpace(roraima_td), 10, 64)

			tocantins_td := tr.ChildText("td:nth-child(10)")
			tocantins, _ := strconv.ParseInt(strings.TrimSpace(tocantins_td), 10, 64)

			area_total := acre + amazonas + amapa + maranhao + mato_grosso + para + rondonia + roraima + tocantins

			// Ignorando empty values
			if len([]rune(referencia_td)) <= 1 {
				return
			}

			if referencia <= 0 {
				l.Warn().
					Str("Runner", runnerName).
					Int64("Valor", referencia).
					Msg("Descartando item da tabela / Valor inválido")

				return
			}

			item := DataDesmatamento{
				Referencia: referencia,
				Acre:       acre,
				Amazonas:   amazonas,
				Amapa:      amapa,
				Maranhao:   maranhao,
				MatoGrosso: mato_grosso,
				Para:       para,
				Rondonia:   rondonia,
				Roraima:    roraima,
				Tocantins:  tocantins,
				AreaTotal:  area_total,
			}

			indice.Data = append(indice.Data, item)
		})
	})

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Efetuando requisição para o Endpoint")

	c.Visit(url)

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Construindo o acomulado")

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
