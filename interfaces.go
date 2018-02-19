package main

type IPublisher interface{
	Publish(*ipfix) error
}