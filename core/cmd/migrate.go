package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/smartcontractkit/chainlink/core/adapters"
	clnull "github.com/smartcontractkit/chainlink/core/null"
	"github.com/smartcontractkit/chainlink/core/services/job"
	"github.com/smartcontractkit/chainlink/core/services/keystore/keys/ethkey"
	"github.com/smartcontractkit/chainlink/core/services/pipeline"
	"github.com/smartcontractkit/chainlink/core/store/models"
	"github.com/tidwall/gjson"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gopkg.in/guregu/null.v4"
)

var requesterWhitelist = []string{"0x0133Aa47B6197D0BA090Bf2CD96626Eb71fFd13c",
	"0x02D5c618DBC591544b19d0bf13543c0728A3c4Ec",
	"0x037E8F2125bF532F3e228991e051c8A7253B642c",
	"0x05Cf62c4bA0ccEA3Da680f9A8744Ac51116D6231",
	"0x0821f21F21C325AE39557CA83B6B4df525495D06",
	"0x1116F76D5717003Ba2Cf2BF80A8789Bf8Fd1b1B6",
	"0x11eF34572CcaB4c85f0BAf03c36a14e0A9C8C7eA",
	"0x151445852B0cfDf6A4CC81440F2AF99176e8AD08",
	"0x16924ae9C2ac6cdbC9D6bB16FAfCD38BeD560936",
	"0x1EC7896DDBfD6af678f0d86cBa859cb7240FC3aE",
	"0x1EeaF25f2ECbcAf204ECADc8Db7B0db9DA845327",
	"0x21f333fd6e4c63Ad826e47fa4249C9Fa18a335c1",
	"0x2408935EFE60F092B442a8755f7572eDb9cF971E",
	"0x25Fa978ea1a7dc9bDc33a2959B9053EaE57169B5",
	"0x28e0fD8e05c14034CbA95C6BF3394d1B106f7Ed8",
	"0x2CbfD29947F774B8cF338f776915e6Fee052f236",
	"0x2De050c0378D32D346A437a01A8272343C5e2409",
	"0x31337027Fb77C8BaD38471589adc7686e65fcf24",
	"0x32dbd3214aC75223e27e575C53944307914F7a90",
	"0x353F61F39a17e56cA413F4559B8cD3b6A252ffC8",
	"0x3E0De81e212eB9ECCD23bb3a9B0E1FAC6C8170fc",
	"0x3dBb9Fa54eFc244e1823B5782Be8a08cC143ea5e",
	"0x3f6E09A4EC3811765F5b2ad15c0279910dbb2c04",
	"0x45e9FEe61185e213c37fc14D18e44eF9262e10Db",
	"0x46Bb139F23B01fef37CB95aE56274804bC3b3e86",
	"0x52D674C76E91c50A0190De77da1faD67D859a569",
	"0x560B06e8897A0E52DbD5723271886BbCC5C1f52a",
	"0x570985649832B51786a181d57BAbe012be1C09a4",
	"0x5d4BB541EED49D0290730b4aB332aA46bd27d888",
	"0x6a6527d91DDaE0a259Cc09DAD311b3455Cdc1fbd",
	"0x6d626Ff97f0E89F6f983dE425dc5B24A18DE26Ea",
	"0x73ead35fd6A572EF763B13Be65a9db96f7643577",
	"0x740be5E8FE30bD2bf664822154b520eae0C565B0",
	"0x759a58A839d00Cd905E4Ae0C29C4c50757860cfb",
	"0x7925998A4A18D141cF348091a7C5823482056fae",
	"0x7AE7781C7F3a5182596d161e037E6db8e36328ef",
	"0x80Eeb41E2a86D4ae9903A3860Dd643daD2D1A853",
	"0x82C5720Cb830341b48AC93Cf6FF3064cF5eB504b",
	"0x8770Afe90c52Fd117f29192866DE705F63e59407",
	"0x8946A183BFaFA95BEcf57c5e08fE5B7654d2807B",
	"0x9b4e2579895efa2b4765063310Dc4109a7641129",
	"0xA0F9D94f060836756FFC84Db4C78d097cA8C23E8",
	"0xA417221ef64b1549575C977764E651c9FAB50141",
	"0xB7B1C8F4095D819BDAE25e7a63393CDF21fd02Ea",
	"0xB836ADc21C241b096A98Dd677eD25a6E3EFA8e94",
	"0xD9d35a82D4dd43BE7cFc524eBf5Cd00c92c48ebC",
	"0xDa3d675d50fF6C555973C4f0424964e1F6A4e7D3",
	"0xE23d1142dE4E83C08bb048bcab54d50907390828",
	"0xF11Bf075f0B2B8d8442AB99C44362f1353D40B44",
	"0xF5fff180082d6017036B771bA883025c654BC935",
	"0xF79D6aFBb6dA890132F9D7c355e3015f15F3406F",
	"0xa6781b4a1eCFB388905e88807c7441e56D887745",
	"0xa7D38FBD325a6467894A13EeFD977aFE558bC1f0",
	"0xa874fe207DF445ff19E7482C746C4D3fD0CB9AcE",
	"0xafcE0c7b7fE3425aDb3871eAe5c0EC6d93E01935",
	"0xb8b513d9cf440C1b6f5C7142120d611C94fC220c",
	"0xc6eE0D4943dc43Bd462145aa6aC95e9C0C8b462f",
	"0xc89c4ed8f52Bb17314022f6c0dCB26210C905C97",
	"0xd0e785973390fF8E77a83961efDb4F271E6B8152",
	"0xd1E850D6afB6c27A3D66a223F6566f0426A6e13B",
	"0xd3CE735cdc708d9607cfbc6C3429861625132cb4",
	"0xdE54467873c3BCAA76421061036053e371721708",
	"0xe1407BfAa6B5965BAd1C9f38316A3b655A09d8A6",
	"0xe2C9aeA66ED352c33f9c7D8e824B7Cac206B0b72",
	"0xeCfA53A8bdA4F0c4dd39c55CC8deF3757aCFDD07",
	"0x0563fC575D5219C48E2Dfc20368FA4179cDF320D",
	"0xf6c446Cb58735c52c35B0a22af13BDb39869D753"}

// MigrateJobSpec - Does not support mixed initiator types.
func MigrateJobSpec(js models.JobSpec) (job.Job, error) {
	var jb job.Job
	if len(js.Initiators) == 0 {
		return jb, errors.New("initiator required to migrate job")
	}
	v1JobType := js.Initiators[0].Type
	switch v1JobType {
	case models.InitiatorCron:
		return migrateCronJob(js)
	case models.InitiatorRunLog:
		return migrateRunLogJob(js)
	default:
		return jb, errors.Wrapf(errors.New("Invalid initiator type"), "%v", v1JobType)
	}
}

func migrateCronJob(js models.JobSpec) (job.Job, error) {
	var jb job.Job
	initr := js.Initiators[0]
	jb = job.Job{
		Name: null.StringFrom(js.Name),
		CronSpec: &job.CronSpec{
			CronSchedule: string(initr.InitiatorParams.Schedule),
			CreatedAt:    js.CreatedAt,
			UpdatedAt:    js.UpdatedAt,
		},
		Type:          job.Cron,
		SchemaVersion: 1,
		ExternalJobID: uuid.NewV4(),
	}
	ps, pd, err := BuildTaskDAG(js, job.Cron)
	if err != nil {
		return jb, err
	}
	jb.PipelineSpec = &pipeline.Spec{
		DotDagSource: ps,
	}
	jb.Pipeline = *pd
	return jb, nil
}

func migrateRunLogJob(js models.JobSpec) (job.Job, error) {
	var jb job.Job
	initr := js.Initiators[0]
	jb = job.Job{
		Name: null.StringFrom(js.Name),
		DirectRequestSpec: &job.DirectRequestSpec{
			ContractAddress:          ethkey.EIP55AddressFromAddress(initr.InitiatorParams.Address),
			MinIncomingConfirmations: clnull.Uint32From(10),
			Requesters:               requesterWhitelist,
			CreatedAt:                js.CreatedAt,
			UpdatedAt:                js.UpdatedAt,
		},
		Type:          job.DirectRequest,
		SchemaVersion: 1,
		ExternalJobID: uuid.NewV4(),
	}
	ps, pd, err := BuildTaskDAG(js, job.DirectRequest)
	if err != nil {
		return jb, err
	}
	jb.PipelineSpec = &pipeline.Spec{
		DotDagSource: ps,
	}
	jb.Pipeline = *pd
	return jb, nil
}

func BuildTaskDAG(js models.JobSpec, tpe job.Type) (string, *pipeline.Pipeline, error) {
	replacements := make(map[string]string)
	dg := pipeline.NewGraph()
	var foundEthTx = false
	var last *pipeline.GraphNode

	if tpe == job.DirectRequest {
		attrs := map[string]string{
			"type":   "ethabidecodelog",
			"abi":    "OracleRequest(bytes32 indexed specId, address requester, bytes32 requestId, uint256 payment, address callbackAddr, bytes4 callbackFunctionId, uint256 cancelExpiration, uint256 dataVersion, bytes32 data)",
			"data":   "$(jobRun.logData)",
			"topics": "$(jobRun.logTopics)",
		}
		n := pipeline.NewGraphNode(dg.NewNode(), "decode_log", attrs)
		dg.AddNode(n)
		last = n

		/*
		   decode_log   [type=ethabidecodelog
		                 abi="OracleRequest(bytes32 indexed specId, address requester, bytes32 requestId, uint256 payment, address callbackAddr, bytes4 callbackFunctionId, uint256 cancelExpiration, uint256 dataVersion, bytes32 data)"
		                 data="$(jobRun.logData)"
		                 topics="$(jobRun.logTopics)"]
		*/
	}

	for i, ts := range js.Tasks {
		var n *pipeline.GraphNode
		switch ts.Type {
		case adapters.TaskTypeHTTPGet:
			mapp := make(map[string]interface{})
			err := json.Unmarshal(ts.Params.Bytes(), &mapp)
			if err != nil {
				return "", nil, err
			}
			marshal, err := json.Marshal(&mapp)
			if err != nil {
				return "", nil, err
			}

			template := fmt.Sprintf("%%REQ_DATA_%v%%", i)
			replacements["\""+template+"\""] = string(marshal)
			attrs := map[string]string{
				"type":        pipeline.TaskTypeHTTP.String(),
				"method":      "GET",
				"requestData": template,
			}
			n = pipeline.NewGraphNode(dg.NewNode(), fmt.Sprintf("http_%d", i), attrs)

		case adapters.TaskTypeJSONParse:
			attrs := map[string]string{
				"type": pipeline.TaskTypeJSONParse.String(),
			}
			if ts.Params.Get("path").Exists() {

				path := ts.Params.Get("path")
				pathString := path.String()

				if path.IsArray() {
					var pathSegments []string
					path.ForEach(func(key, value gjson.Result) bool {
						pathSegments = append(pathSegments, value.String())
						return true
					})

					pathString = strings.Join(pathSegments, ",")
				}

				attrs["path"] = pathString
			} else {
				return "", nil, errors.New("no path param on jsonparse task")
			}
			n = pipeline.NewGraphNode(dg.NewNode(), fmt.Sprintf("jsonparse_%d", i), attrs)

		case adapters.TaskTypeMultiply:
			attrs := map[string]string{
				"type": pipeline.TaskTypeMultiply.String(),
			}
			if ts.Params.Get("times").Exists() {
				attrs["times"] = ts.Params.Get("times").String()
			} else {
				return "", nil, errors.New("no times param on multiply task")
			}
			n = pipeline.NewGraphNode(dg.NewNode(), fmt.Sprintf("multiply_%d", i), attrs)
		case adapters.TaskTypeEthUint256, adapters.TaskTypeEthInt256:
			// Do nothing. This is implicit in FMv2 / DR
		case adapters.TaskTypeEthTx:
			if tpe == job.DirectRequest {
				attrs := map[string]string{
					"type": "ethabiencode",
					"abi":  "(uint256 value)",
					//"data": <{ "value": $(multiply) }>,
				}
				n = pipeline.NewGraphNode(dg.NewNode(), fmt.Sprintf("encode_data_%d", i), attrs)
				dg.AddNode(n)
				if last != nil {
					dg.SetEdge(dg.NewEdge(last, n))
				}
				last = n
			}
			if tpe == job.DirectRequest {

				template := fmt.Sprintf("%%REQ_DATA_%v%%", i)
				attrs := map[string]string{
					"type": "ethabiencode",
					"abi":  "fulfillOracleRequest(bytes32 requestId, uint256 payment, address callbackAddress, bytes4 callbackFunctionId, uint256 expiration, bytes32 calldata data)",
					"data": template,
				}
				replacements["\""+template+"\""] = `{
"requestId":          $(decode_log.requestId),
"payment":            $(decode_log.payment),
"callbackAddress":    $(decode_log.callbackAddr),
"callbackFunctionId": $(decode_log.callbackFunctionId),
"expiration":         $(decode_log.cancelExpiration),
"data":               $(encode_data)
}
`

				n = pipeline.NewGraphNode(dg.NewNode(), fmt.Sprintf("encode_tx_%d", i), attrs)
				dg.AddNode(n)
				if last != nil {
					dg.SetEdge(dg.NewEdge(last, n))
				}
				last = n
			}
			attrs := map[string]string{
				"type": pipeline.TaskTypeETHTx.String(),
				"to":   js.Initiators[0].Address.String(),
				"data": fmt.Sprintf("$(%v)", last.DOTID()),
			}
			n = pipeline.NewGraphNode(dg.NewNode(), fmt.Sprintf("send_tx_%d", i), attrs)
			foundEthTx = true
		default:
			// assume it's a bridge task
			encodedValue, err := encodeTemplate(ts.Params.Bytes())
			if err != nil {
				return "", nil, err
			}
			template := fmt.Sprintf("%%REQ_DATA_%v%%", i)
			attrs := map[string]string{
				"type":        pipeline.TaskTypeBridge.String(),
				"name":        ts.Type.String(),
				"requestData": template,
			}
			replacements["\""+template+"\""] = encodedValue

			n = pipeline.NewGraphNode(dg.NewNode(), fmt.Sprintf("send_to_bridge_%d", i), attrs)
			i++
		}

		if n != nil {
			dg.AddNode(n)
			if last != nil {
				dg.SetEdge(dg.NewEdge(last, n))
			}
			last = n
		}
	}
	if !foundEthTx && tpe == job.DirectRequest {
		return "", nil, errors.New("expected ethtx in FM v1 / Runlog job spec")
	}

	s, err := dot.Marshal(dg, "", "", "")
	if err != nil {
		return "", nil, err
	}

	// Double check we can unmarshal it
	generatedDotDagSource := string(s)
	generatedDotDagSource = strings.Replace(generatedDotDagSource, "strict digraph {", "", 1)
	generatedDotDagSource = strings.Replace(generatedDotDagSource, "\n// Node definitions.\n", "", 1)
	generatedDotDagSource = strings.Replace(generatedDotDagSource, "\n", "\n\t", 100)

	for key := range replacements {
		generatedDotDagSource = strings.Replace(generatedDotDagSource, key, "<"+replacements[key]+">", 1)
	}
	generatedDotDagSource = generatedDotDagSource[:len(generatedDotDagSource)-1] // Remove final }
	p, err := pipeline.Parse(generatedDotDagSource)
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to generate pipeline from: \n%v", generatedDotDagSource)
	}
	return generatedDotDagSource, p, err
}

func encodeTemplate(bytes []byte) (string, error) {
	mapp := make(map[string]interface{})
	err := json.Unmarshal(bytes, &mapp)
	if err != nil {
		return "", err
	}
	marshal, err := json.Marshal(&mapp)
	if err != nil {
		return "", err
	}
	return string(marshal), nil
}
