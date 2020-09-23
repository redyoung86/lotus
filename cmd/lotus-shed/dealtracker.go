package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
)

type dealStatsServer struct {
	api api.FullNode
}

var walletCache map[address.Address]address.Address
var knownFilteredClients map[address.Address]bool

func init() {
	walletCache = make(map[address.Address]address.Address, 4096)

	knownFilteredClients = make(map[address.Address]bool)
	for _, a := range []string{
		"t0100",
		"t0112",
		"t0113",
		"t0114",
		"t010089",
	} {
		addr, _ := address.NewFromString(a)
		knownFilteredClients[addr] = true
	}
}

type dealData struct {
	deal   api.MarketDeal
	wallet address.Address
}

func (dss *dealStatsServer) dealList() (int64, map[string]dealData) {
	ctx := context.Background()

	head, err := dss.api.ChainHead(ctx)
	if err != nil {
		log.Warnf("failed to get chain head: %s", err)
		return 0, nil
	}

	miners, err := dss.api.StateListMiners(ctx, head.Key())
	if err != nil {
		log.Warnf("failed to get miner list: %s", err)
		return 0, nil
	}
	for _, m := range miners {
		info, _ := dss.api.StateMinerInfo(ctx, m, head.Key())
		knownFilteredClients[info.Owner] = true
		knownFilteredClients[info.Worker] = true
		for _, a := range info.ControlAddresses {
			knownFilteredClients[a] = true
		}
	}

	deals, err := dss.api.StateMarketDeals(ctx, head.Key())
	if err != nil {
		log.Warnf("failed to get market deals: %s", err)
		return 0, nil
	}

	ret := make(map[string]dealData, len(deals))
	for _, d := range deals {

		// Counting of non-existent deals diabled as per Pooja's request
		// // https://github.com/filecoin-project/specs-actors/blob/v0.9.9/actors/builtin/market/deal.go#L81-L85
		// if d.State.SectorStartEpoch < 0 {
		// 	continue
		// }

		dw := dealData{deal: d}
		if _, found := walletCache[d.Proposal.Client]; !found {
			walletCache[d.Proposal.Client], _ = dss.api.StateAccountKey(ctx, d.Proposal.Client, head.Key())
		}
		dw.wallet = walletCache[d.Proposal.Client]

		if knownFilteredClients[d.Proposal.Client] {
			continue
		}

		pCid, _ := d.Proposal.Cid()
		ret[pCid.String()] = dw
	}

	return int64(head.Height()), ret
}

type dealCountResp struct {
	Epoch    int64  `json:"epoch"`
	Endpoint string `json:"endpoint"`
	Payload  int64  `json:"payload"`
}

func (dss *dealStatsServer) handleStorageDealCount(w http.ResponseWriter, r *http.Request) {

	epoch, deals := dss.dealList()
	if epoch == 0 {
		w.WriteHeader(500)
		return
	}

	if err := json.NewEncoder(w).Encode(&dealCountResp{
		Endpoint: "COUNT_DEALS",
		Payload:  int64(len(deals)),
		Epoch:    epoch,
	}); err != nil {
		log.Warnf("failed to write back deal count response: %s", err)
		return
	}
}

type dealAverageResp struct {
	Epoch    int64  `json:"epoch"`
	Endpoint string `json:"endpoint"`
	Payload  int64  `json:"payload"`
}

func (dss *dealStatsServer) handleStorageDealAverageSize(w http.ResponseWriter, r *http.Request) {

	epoch, deals := dss.dealList()
	if epoch == 0 {
		w.WriteHeader(500)
		return
	}

	var totalBytes int64
	for _, d := range deals {
		totalBytes += int64(d.deal.Proposal.PieceSize.Unpadded())
	}

	if err := json.NewEncoder(w).Encode(&dealAverageResp{
		Endpoint: "AVERAGE_DEAL_SIZE",
		Payload:  totalBytes / int64(len(deals)),
		Epoch:    epoch,
	}); err != nil {
		log.Warnf("failed to write back deal average response: %s", err)
		return
	}
}

type dealTotalResp struct {
	Epoch    int64  `json:"epoch"`
	Endpoint string `json:"endpoint"`
	Payload  int64  `json:"payload"`
}

func (dss *dealStatsServer) handleStorageDealTotalReal(w http.ResponseWriter, r *http.Request) {

	epoch, deals := dss.dealList()
	if epoch == 0 {
		w.WriteHeader(500)
		return
	}

	var totalBytes int64
	for _, d := range deals {
		totalBytes += int64(d.deal.Proposal.PieceSize.Unpadded())
	}

	if err := json.NewEncoder(w).Encode(&dealTotalResp{
		Endpoint: "DEAL_BYTES",
		Payload:  totalBytes,
		Epoch:    epoch,
	}); err != nil {
		log.Warnf("failed to write back deal average response: %s", err)
		return
	}

}

type clientStatsOutput struct {
	Epoch    int64          `json:"epoch"`
	Endpoint string         `json:"endpoint"`
	Payload  []*clientStats `json:"payload"`
}

type clientStats struct {
	Client    address.Address `json:"client"`
	DataSize  int64           `json:"data_size"`
	NumCids   int             `json:"num_cids"`
	NumDeals  int             `json:"num_deals"`
	NumMiners int             `json:"num_miners"`

	cids      map[cid.Cid]bool
	providers map[address.Address]bool
}

func (dss *dealStatsServer) handleStorageClientStats(w http.ResponseWriter, r *http.Request) {
	epoch, deals := dss.dealList()
	if epoch == 0 {
		w.WriteHeader(500)
		return
	}

	stats := make(map[address.Address]*clientStats)

	for _, d := range deals {
		st, ok := stats[d.deal.Proposal.Client]
		if !ok {
			st = &clientStats{
				Client:    d.wallet,
				cids:      make(map[cid.Cid]bool),
				providers: make(map[address.Address]bool),
			}
			stats[d.deal.Proposal.Client] = st
		}

		st.DataSize += int64(d.deal.Proposal.PieceSize.Unpadded())
		st.cids[d.deal.Proposal.PieceCID] = true
		st.providers[d.deal.Proposal.Provider] = true
		st.NumDeals++
	}

	out := clientStatsOutput{
		Epoch:    epoch,
		Endpoint: "CLIENT_DEAL_STATS",
		Payload:  make([]*clientStats, 0, len(stats)),
	}

	for _, cso := range stats {
		cso.NumCids = len(cso.cids)
		cso.NumMiners = len(cso.providers)

		out.Payload = append(out.Payload, cso)
	}

	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Warnf("failed to write back client stats response: %s", err)
		return
	}
}

var serveDealStatsCmd = &cli.Command{
	Name:  "serve-deal-stats",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}

		defer closer()
		ctx := lcli.ReqContext(cctx)

		_ = ctx

		dss := &dealStatsServer{api}

		mux := &http.ServeMux{}
		mux.HandleFunc("/api/storagedeal/count", dss.handleStorageDealCount)
		mux.HandleFunc("/api/storagedeal/averagesize", dss.handleStorageDealAverageSize)
		mux.HandleFunc("/api/storagedeal/totalreal", dss.handleStorageDealTotalReal)
		mux.HandleFunc("/api/storagedeal/clientstats", dss.handleStorageClientStats)

		s := &http.Server{
			Addr:    ":7272",
			Handler: mux,
		}

		go func() {
			<-ctx.Done()
			s.Shutdown(context.TODO())
		}()

		list, err := net.Listen("tcp", ":7272")
		if err != nil {
			panic(err)
		}

		s.Serve(list)
		return nil
	},
}
