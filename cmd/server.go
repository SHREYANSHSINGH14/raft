package cmd

import (
	"github.com/SHREYANSHSINGH14/raft/raft"
	"github.com/spf13/cobra"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Server related commands",
	Long:  `Commands related to server operations like starting and stopping the server`,
}

var startCmd = &cobra.Command{
	Use: "start",
	Run: func(cmd *cobra.Command, args []string) {
		var config raft.Config
		var err error
		config.LoadConfig()

		server, err := raft.NewServer(cmd.Context(), config)
		if err != nil {
			panic("error creating server: " + err.Error())
		}
		if server == nil {
			panic("server is not initialized")
		}
		server.Start()
	},
}

func init() {
	serverCmd.AddCommand(startCmd)
}
