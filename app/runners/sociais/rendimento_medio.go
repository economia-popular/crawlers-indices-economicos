package sociais

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

type RendimentoResponse []struct {
	NtCod  string `json:"nt_cod"`
	Nt     string `json:"nt"`
	UgCod  string `json:"ug_cod"`
	Ug     string `json:"ug"`
	PCod   string `json:"p_cod"`
	P      string `json:"p"`
	VarCod string `json:"var_cod"`
	Var    string `json:"var"`
	UmCod  string `json:"um_cod"`
	Um     string `json:"um"`
	V      string `json:"v"`
}

type DataRendimento struct {
	Referencia string  `json:"referencia" csv:"referencia"`
	Ano        string  `json:"ano" csv:"ano"`
	Mes        string  `json:"mes" csv:"mes"`
	Variacao   float64 `json:"rendimento_medio" csv:"rendimento_medio"`
}

type Rendimento struct {
	Atualizacao   time.Time        `json:"data_atualizacao"`
	UnidadeMedida string           `json:"unidade_medida"`
	Fonte         string           `json:"fonte"`
	Data          []DataRendimento `json:"data"`
}

func RunnerRendimento() {
	runnerName := "Sociais - Rendimento Médio Mensal"

	url := "https://servicodados.ibge.gov.br/api/v1/conjunturais?&d=s&user=ibge&t=6390&v=5933&p=-1000&ng=1(1)&c="
	unidadeMedida := "%"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/sociais/rendimento_medio.json"
	fileNameOutputCSV := "./data/sociais/rendimento_medio.csv"

	s3KeyCSV := "sociais/rendimento_medio.csv"
	s3KeyJSON := "sociais/rendimento_medio.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	rendimento := Rendimento{}
	now := time.Now()
	rendimento.Atualizacao = now
	rendimento.Fonte = fonte
	rendimento.UnidadeMedida = unidadeMedida

	var response RendimentoResponse

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

	for _, v := range response {
		ano := v.PCod[:4]
		mes := v.PCod[len(v.PCod)-2:]
		referencia := fmt.Sprintf("%v-%v", ano, mes)
		valor, _ := strconv.ParseFloat(strings.TrimSpace(v.V), 64)

		item := DataRendimento{
			Ano:        ano,
			Mes:        mes,
			Referencia: referencia,
			Variacao:   valor,
		}

		rendimento.Data = append(rendimento.Data, item)
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(rendimento)
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

	csvOutput, err := gocsv.MarshalString(&rendimento.Data)
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

}
