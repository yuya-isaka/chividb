
# 静的解析のフラグを設定
VET_FLAGS := -all -vettool=$(shell command -v shadow)

# テスト用のフラグを設定
TEST_FLAGS := -race -covermode=atomic

# 静的解析とテストを実行
test:
	go vet $(VET_FLAGS) ./...
	gotest $(TEST_FLAGS) ./...
