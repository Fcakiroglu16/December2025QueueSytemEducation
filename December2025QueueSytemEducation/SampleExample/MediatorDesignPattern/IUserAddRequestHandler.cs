using System;
using System.Collections.Generic;
using System.Text;

namespace December2025QueueSystemEducation.SampleExample.MediatorDesignPattern
{
    public interface IUserAddRequestHandler
    {
        void Handle(User user);
    }
}
