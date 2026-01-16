// Repository/DAO,Data access object
package repo

import (
	"time" //IsZero()

	"github.com/charlesAcmen/livestream-danmaku/internal/model"

	"gorm.io/gorm"
)

// MessageRepo handles database operations for danmu messages.
type MessageRepo struct {
	db *gorm.DB
}

// NewMessageRepo creates a new repository instance.
func NewMessageRepo(db *gorm.DB) *MessageRepo {
	return &MessageRepo{db: db}
}

// CreateInBatches saves multiple messages efficiently.
// Used by the Kafka Consumer.
func (r *MessageRepo) CreateInBatches(msgs []*model.DanmakuMessage) error {
	return r.db.CreateInBatches(msgs, len(msgs)).Error
}

// GetPlayBackMsgByRoomID fetches messages for video playback
// (VOD video on demand mode).
// It retrieves messages that happened AFTER specific timestamp.
// queryTime: The current timestamp of the video player progress.
func (r *MessageRepo) GetPlayBackMsgByRoomID(roomID string, queryTime time.Time, limit int) ([]*model.DanmakuMessage, error) {
	var msgs []*model.DanmakuMessage

	//1.init SQL builder
	//SELECT * FROM danmaku_messages WHERE rood_id = `1001`
	query := r.db.Where("room_id = ?", roomID)

	//"0001-01-01 00:00:00" empty time
	if !queryTime.IsZero() {
		// Cursor Pagination: Fetch messages OLDER than the cursor (History scroll up)
		// Or NEWER than the cursor (Playback forward).
		// Let's assume typical history load: Load latest N messages.
		query = query.Where("send_time >= ?", queryTime)
	}

	// Order by time ASC (oldest first)
	err := query.Order("send_time ASC").
		Limit(limit).
		//Find will execute SQL
		Find(&msgs).Error
	// SELECT * FROM danmaku_messages
	// WHERE room_id = '1001' AND send_time >= '...'
	// ORDER BY send_time ASC
	// LIMIT 20;

	return msgs, err

}
