package cidlocation

import (
	"github.com/CIDgravity/filecoin-gateway/iface"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
)

var log = logging.Logger("gw/rbstor/cidlocation")

var OffloadStatusStaging = iface.OffloadStatus("staging")
var OffloadStatusComplete = iface.OffloadStatus("complete")

var Module = fx.Module(
	"cidlocation",
	fx.Provide(NewWorker, NewCidLocationIndex),
	//fx.Invoke(StartWorker),
)
