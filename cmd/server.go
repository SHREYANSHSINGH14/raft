package cmd

import (
	"github.com/SHREYANSHSINGH14/raft/config"
	"github.com/SHREYANSHSINGH14/raft/server"
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
		var config config.Config
		var err error
		config.LoadConfig()

		server, err := server.NewServer(cmd.Context(), config)
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
