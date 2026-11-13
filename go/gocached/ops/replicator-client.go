package ops

import (
	"replication/replicator_client"
	"time"
)

func init() {

	go func() {

		for {
			time.Sleep(20 * time.Millisecond)

			_toprocess := replicator_client.GetPendingOps()
			if _toprocess == nil {
				continue
			}

			for _, v := range _toprocess {
				switch {
				case v.Operation == SYNC_KEY_TOUCH:
					OpSetRawForReplicator(v.Key, nil, v.Expires)
				case v.Operation == SYNC_KEY_SET:
					OpSetRawForReplicator(v.Key, v.Data, v.Expires)
				case v.Operation == SYNC_KEY_DELETE:
					OpSetRawForReplicator(v.Key, nil, 0)
				case v.Operation == SYNC_FLUSH:
					OpFlush(false)
				}
			}

		}
	}()
}
