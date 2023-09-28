package ambientais

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/gocarina/gocsv"
)

type DataPoluentesAtmosfericosEstadosResumido struct {
	Referencia string  `json:"referencia" csv:"referencia"`
	Estado     string  `json:"estado" csv:"estado"`
	Poluente   string  `json:"poluente_emitido" csv:"poluente_emitido"`
	Quantidade float64 `json:"quantidade" csv:"quantidade"`
}

type PoluentesAtmosfericosEstadosResumido struct {
	Atualizacao time.Time                                  `json:"data_atualizacao"`
	Fonte       string                                     `json:"fonte"`
	Data        []DataPoluentesAtmosfericosEstadosResumido `json:"data"`
}

func RunnerPoluentesAtmosfericosEstadosResumido(emissores PoluentesAtmosfericosEmpresas) {
	runnerName := "Poluentes Atmosféricos - Estados Resumido"

	l := logger.Instance()

	index := PoluentesAtmosfericosEstadosResumido{}
	now := time.Now()
	index.Atualizacao = now
	index.Fonte = emissores.Fonte

	file_path := "./data/ambientais/estados_emissao_poluentes_resumido.json"
	fileNameOutputCSV := "./data/ambientais/estados_emissao_poluentes_resumido.csv"

	s3KeyCSV := "ambientais/estados_emissao_poluentes_resumido.csv"
	s3KeyJSON := "ambientais/estados_emissao_poluentes_resumido.csv"

	ordenado := make(map[string]DataPoluentesAtmosfericosEstadosResumido)

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	// Mapa de Referencias
	for _, v := range emissores.Data {
		i := fmt.Sprintf("%s%s", v.Referencia, v.Estado)
		d := DataPoluentesAtmosfericosEstadosResumido{
			Referencia: v.Referencia,
			Estado:     v.Estado,
			Quantidade: 0,
		}
		ordenado[i] = d
	}

	for _, v := range emissores.Data {
		i := fmt.Sprintf("%s%s", v.Referencia, v.Estado)
		item := ordenado[i]
		item.Quantidade = item.Quantidade + v.Quantidade
		ordenado[i] = item
	}

	for _, i := range ordenado {
		index.Data = append(index.Data, i)
	}

	// Sort do data
	sort.Slice(index.Data, func(i, j int) bool {
		return index.Data[i].Referencia < index.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(index)
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

	csvOutput, err := gocsv.MarshalString(&index.Data)
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
