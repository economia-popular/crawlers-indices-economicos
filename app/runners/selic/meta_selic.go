package selic

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/gocarina/gocsv"
)

type DataMeta struct {
	NumeroReuniao      float64 `json:"numero_reuniao" csv:"numero_reuniao"`
	Periodo            string  `json:"periodo" csv:"periodo"`
	MesAno             string  `json:"mes_ano" csv:"mes_ano"`
	DataReuniao        string  `json:"data_reuniao" csv:"data_reuniao"`
	DataInicioVigencia string  `json:"inicio_vigencia" csv:"inicio_vigencia"`
	// DataFimVigencia    string  `json:"fim_vigencia" csv:"fim_vigencia"`
	Valor float64 `json:"valor" csv:"valor"`
	Vies  string  `json:"vies" csv:"vies"`
}

type SELICMeta struct {
	Atualizacao   time.Time  `json:"data_atualizacao"`
	UnidadeMedida string     `json:"unidade_medida"`
	Fonte         string     `json:"fonte"`
	Data          []DataMeta `json:"data"`
}

type ResponseMetaSelic struct {
	Conteudo []struct {
		NumeroReuniaoCopom         float64     `json:"NumeroReuniaoCopom"`
		ReuniaoExtraordinaria      bool        `json:"ReuniaoExtraordinaria"`
		DataReuniaoCopom           time.Time   `json:"DataReuniaoCopom"`
		Vies                       string      `json:"Vies"`
		UsoMetaSelic               bool        `json:"UsoMetaSelic"`
		DataInicioVigencia         time.Time   `json:"DataInicioVigencia"`
		DataFimVigencia            time.Time   `json:"DataFimVigencia"`
		MetaSelic                  float64     `json:"MetaSelic"`
		TaxaTban                   interface{} `json:"TaxaTban"`
		TaxaSelicEfetivaVigencia   interface{} `json:"TaxaSelicEfetivaVigencia"`
		TaxaSelicEfetivaAnualizada interface{} `json:"TaxaSelicEfetivaAnualizada"`
	} `json:"conteudo"`
}

func RunnerMetaSelic() {
	runnerName := "SELIC Meta"
	url := "https://www.bcb.gov.br/api/servico/sitebcb/historicotaxasjuros"
	unidadeMedida := "Taxa de Juros"
	fonte := "https://www.bcb.gov.br"
	file_path := "./data/selic/selic-meta.json"
	fileNameOutputCSV := "./data/selic/selic-meta.csv"

	s3KeyCSV := "selic/selic-meta.csv"
	s3KeyJSON := "selic/selic-meta.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	selic := &SELICMeta{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	selic.Atualizacao = now
	selic.Fonte = fonte
	selic.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Realizando o request na API do bcbdata para recuperar a série histórica da SELIC")

	res, err := http.Get(url)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTO para o endpoint dos dados")
		return
	}

	defer res.Body.Close()

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	var response ResponseMetaSelic

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	for _, d := range response.Conteudo {

		periodo := d.DataReuniaoCopom.Format("2006-01")
		dataReuniao := d.DataReuniaoCopom.Format("2006-01-02")
		dataInicioVigencia := d.DataInicioVigencia.Format("2006-01-02")
		mesAno := d.DataReuniaoCopom.Format("Jan/2006")

		item := DataMeta{
			NumeroReuniao:      d.NumeroReuniaoCopom,
			Periodo:            periodo,
			DataReuniao:        dataReuniao,
			DataInicioVigencia: dataInicioVigencia,
			MesAno:             mesAno,
			Vies:               d.Vies,
			Valor:              d.MetaSelic,
		}

		// Removendo Itens Inválidos
		if item.Periodo != "0001-01" {
			selic.Data = append(selic.Data, item)
		}

	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Revertendo a ordem do slice")

	var output []DataMeta

	input := selic.Data

	for i := len(input) - 1; i >= 0; i-- {
		output = append(output, input[i])
	}

	selic.Data = output

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(selic)
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

	csvOutput, err := gocsv.MarshalString(&selic.Data)
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
		Str("FilePath", file_path).
		Msg("Finalizado")
}
