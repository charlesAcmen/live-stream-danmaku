// Repository/DAO,Data access object
package repo

import (
	"time"

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

// GetHistoryByRoomID fetches historical messages for a room.
// Implementation of "YouTube-style" playback: fetch N messages after a specific time.
// If lastTime is zero, fetch the latest messages (live mode initial load).
func (r *MessageRepo) GetHistoryByRoomID(roomID string, lastTime time.Time, limit int) ([]*model.DanmakuMessage, error) {
	var msgs []*model.DanmakuMessage

	query := r.db.Where("room_id = ?", roomID)

	if !lastTime.IsZero() {
		// Pagination: Fetch messages OLDER than the cursor (History scroll up)
		// Or NEWER than the cursor (Playback forward).
		// Let's assume typical history load: Load latest N messages.
		query = query.Where("send_time < ?", lastTime)
	}

	// Order by time DESC (latest first) so we get the most recent ones.
	// Frontend will reverse array to show timeline.
	err := query.Order("send_time DESC").
		Limit(limit).
		Find(&msgs).Error

	return msgs, err
}
