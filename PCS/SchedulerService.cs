
using System;
using System.Collections.Generic;
using System.Threading;

namespace PCS
{
    public class SchedulerService
    {
        private static SchedulerService _instance;
        private List<Timer> timers = new List<Timer>();

        private SchedulerService() { }

        public static SchedulerService Instance => _instance ?? (_instance = new SchedulerService());

        public void ScheduleTask(double intervalInHour, Action task)
        {
            var timer = new Timer(x =>
            {
                task.Invoke();
            }, null, TimeSpan.Zero, TimeSpan.FromHours(intervalInHour));

            timers.Add(timer);
        }
    }
}
