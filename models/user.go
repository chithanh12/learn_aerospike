package models

type User struct {
	ID      string `as:"id"`
	Name    string `as:"name"`
	Age     int    `as:"age"`
	Address string `as:"address"`
}
