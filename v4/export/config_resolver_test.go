package export

import (
	"context"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigResolverSuite{})

type testConfigResolverSuite struct{}

func (s *testConfigResolverSuite) TestConfigResolver(c *C) {
	conf := DefaultConfig()

	conf.Where = "id < 5"
	conf.Sql = "select * from t where id > 3"
	cr := NewConfigResolver(context.Background())
	err := cr.ResolveConfig(conf)
	c.Assert(err, ErrorMatches, "can't specify both --sql and --where at the same time. Please try to combine them into --sql")

	conf.Where = ""
	err = cr.ResolveConfig(conf)
	c.Assert(err, IsNil)

	conf.Sql = ""
	conf.Rows = 5000
	conf.FileSize = uint64(5000)
	err = cr.ResolveConfig(conf)
	c.Assert(err, ErrorMatches, "invalid config: cannot set both --rows and --filesize to non-zero")
}
