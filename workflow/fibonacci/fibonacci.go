// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package helloworld

import (
	"context"
	"time"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

const (
	WorkflowName = "Fibonacci"
	ActivityName = "FibonacciActivity"
)

// RegisterWorkflow registers the HelloWorldWorkflow.
func RegisterWorkflow(w worker.Worker) {
	w.RegisterWorkflowWithOptions(FibonacciWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	w.RegisterActivityWithOptions(FibonacciActivity, activity.RegisterOptions{
		Name:                ActivityName,
		EnableAutoHeartbeat: true,
	})
}

type fibInput struct {
	value int `int:"value"`
}

// HelloWorldWorkflow greets the caller.
func FibonacciWorkflow(ctx workflow.Context, input fibInput) (string, error) {
	ao := workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("FibonacciWorkflow started")

	var greetingMsg string
	err := workflow.ExecuteActivity(ctx, FibonacciActivity, input).Get(ctx, &greetingMsg)
	if err != nil {
		logger.Error("FibonacciActivity failed", zap.Error(err))
		return "", err
	}

	logger.Info("Workflow result", zap.String("greeting", greetingMsg))
	return greetingMsg, nil
}

// Calc fibonacci number Compute heavy task
func FibonacciActivity(ctx context.Context, input fibInput) {

}
