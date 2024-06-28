using System.Data;

namespace Application.Validators.Interfaces;

internal interface IValidator
{
    void ValidateProviders(IDbConnection source, IDbConnection dest);
}
